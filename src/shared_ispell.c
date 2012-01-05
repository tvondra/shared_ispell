#include <stdio.h>
#include <stdlib.h>
#include <sys/shm.h>
#include <sys/stat.h>

#include <sys/types.h>
#include <sys/ipc.h>

#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/fd.h"

#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "utils/guc.h"
#include "commands/defrem.h"
#include "tsearch/ts_locale.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"

#include "funcapi.h"

#include "libpq/md5.h"

#include "spell.h"
#include "tsearch/dicts/spell.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#if (PG_VERSION_NUM < 90100)
#define	NIStartBuild(dict)
#define	NIFinishBuild(dict)
#endif

/* private functions */
static void ispell_shmem_startup(void);

/* This segment is initialized in the first process that accesses it (see
 * ispell_shmem_startup function).
 */
#define SEGMENT_NAME    "shared_ispell"

static int  max_ispell_mem_size  = (30*1024*1024); /* 50MB by default */

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

void        _PG_init(void);
void		_PG_fini(void);

/* used to allocate memory in the shared segment */
typedef struct SegmentInfo {
	
	LWLockId	lock;
	char		*firstfree;		/* first free address (always maxaligned) */
	size_t		available;		/* free space remaining at firstfree */
	Timestamp	lastReset;		/* last reset of the dictionary */
	
	/* the shared segment (info and data) */
	SharedIspellDict * dict;
	SharedStopList   * stop;
	
} SegmentInfo;

#define MAXLEN 255

/* used to keep track of dictionary in each backend */
typedef struct DictInfo {

	Timestamp	lookup;
	
	char dictFile[MAXLEN];
	char affixFile[MAXLEN];
	char stopFile[MAXLEN];

	SharedIspellDict * dict;
	SharedStopList   * stop;

} DictInfo;

/* These are used to allocate data within shared segment */
static SegmentInfo * segment_info = NULL;

static char * shalloc(int bytes);

static SharedIspellDict * copyIspellDict(IspellDict * dict, char * dictFile, char * affixFile, int bytes, int words);
static SharedStopList * copyStopList(StopList * list, char * stopFile, int bytes);

static int sizeIspellDict(IspellDict * dict, char * dictFile, char * affixFile);
static int sizeStopList(StopList * list, char * stopFile);

/*
 * Module load callback
 */
void
_PG_init(void)
{
		
    /* */
    if (! process_shared_preload_libraries_in_progress) {
		elog(ERROR, "shared_ispell has to be loaded using shared_preload_libraries");
        return;
	}
    
    /* Define custom GUC variables. */
    DefineCustomIntVariable("shared_ispell.max_size",
                            "Max amount of memory to allocate for ispell dictionaries.",
                            NULL,
                            &max_ispell_mem_size,
                            (30*1024*1024),
                            (1024*1024), INT_MAX,
                            PGC_POSTMASTER,
                            GUC_UNIT_BLOCKS,
#if (PG_VERSION_NUM >= 90100)
                            NULL,
#endif
                            NULL,
                            NULL);

    EmitWarningsOnPlaceholders("shared_ispell");
	
    /*
     * Request additional shared resources.  (These are no-ops if we're not in
     * the postmaster process.)  We'll allocate or attach to the shared
     * resources in ispell_shmem_startup().
     */
    RequestAddinShmemSpace(max_ispell_mem_size);
    RequestAddinLWLocks(1);
	
    /* Install hooks. */
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = ispell_shmem_startup;
    
}


/*
 * Module unload callback
 */
void
_PG_fini(void)
{
    /* Uninstall hooks. */
    shmem_startup_hook = prev_shmem_startup_hook;
}


/* This is probably the most important part - allocates the shared 
 * segment, initializes it etc. */
static
void ispell_shmem_startup() {

    bool found = FALSE;
	char * segment;
        
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();
    /*
     * Create or attach to the shared memory state, including hash table
     */
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	
    segment = ShmemInitStruct(SEGMENT_NAME,
								  max_ispell_mem_size,
								  &found);
	
    elog(DEBUG1, "initializing shared ispell segment (size: %d B)",
				 max_ispell_mem_size);

    if (! found) {
		
		memset(segment, 0, max_ispell_mem_size);
		
		segment_info = (SegmentInfo*)segment;
		
		segment_info->lock  = LWLockAssign();
		segment_info->firstfree = segment + MAXALIGN(sizeof(SegmentInfo));
		segment_info->available = max_ispell_mem_size - (int)(segment_info->firstfree - segment);
		
		segment_info->lastReset = GetCurrentTimestamp();
        
        elog(DEBUG1, "shared memory segment (shared ispell) successfully created");
        
    }
    
    LWLockRelease(AddinShmemInitLock);
    
}

static
SharedIspellDict * get_shared_dict(char * words, char * affixes) {
	
	SharedIspellDict * dict = segment_info->dict;
	
	while (dict != NULL) {
		if ((strcmp(dict->dictFile, words) == 0) &&
			(strcmp(dict->affixFile, affixes) == 0)) {
			return dict;
		}
		dict = dict->next;
	}
	
	return NULL;
}

static
SharedStopList * get_shared_stop_list(char * stop) {
	
	SharedStopList * list = segment_info->stop;
	
	while (list != NULL) {
		if (strcmp(list->stopFile, stop) == 0) {
			return list;
		}
		list = list->next;
	}
	
	return NULL;
}

static
void init_shared_dict(DictInfo * info, char * dictFile, char * affFile, char * stopFile) {
	
	int size;
	
	SharedIspellDict * shdict;
	SharedStopList * shstop;
	
	IspellDict * dict;
	StopList	stoplist;

	/* dictionary (words and affixes) */
	shdict = get_shared_dict(dictFile, affFile);
	
	/* init if needed */
	if (shdict == NULL) {
		
		dict = (IspellDict *)palloc0(sizeof(IspellDict));
		
		NIStartBuild(dict);
		
		NIImportDictionary(dict,
						get_tsearch_config_filename(dictFile, "dict"));
		
		NIImportAffixes(dict,
						get_tsearch_config_filename(affFile, "affix"));
		
		NISortDictionary(dict);
		NISortAffixes(dict);

		NIFinishBuild(dict);

		/* check available space in shared segment */
		size = sizeIspellDict(dict, dictFile, affFile);
		if (size > segment_info->available) {
			elog(ERROR, "shared dictionary %s.dict / %s.affix needs %d B, only %ld B available",
				 dictFile, affFile, size, segment_info->available);
		}

		/* fine, there's enough space - copy the dictionary */
		shdict = copyIspellDict(dict, dictFile, affFile, size, dict->nspell);
		
		elog(INFO, "shared dictionary %s.dict / %s.affix loaded, used %d B, %ld B remaining",
				 dictFile, affFile, size, segment_info->available);
		
		shdict->next = segment_info->dict;
		segment_info->dict = shdict;
		
	}
	
	/* stop words */
	shstop = get_shared_stop_list(stopFile);
	if ((shstop == NULL) && (stopFile != NULL)) {
		
		readstoplist(stopFile, &stoplist, lowerstr);
		
		size = sizeStopList(&stoplist, stopFile);
		if (size > segment_info->available) {
			elog(ERROR, "shared stoplist %s.stop needs %d B, only %ld B available",
				 stopFile, size, segment_info->available);
		}
		
		/* fine, there's enough space - copy the stoplist */
		shstop = copyStopList(&stoplist, stopFile, size);
		
		elog(INFO, "shared stoplist %s.stop loaded, used %d B, %ld B remaining",
				 affFile, size, segment_info->available);
		
		shstop->next = segment_info->stop;
		segment_info->stop = shstop;
		
	}
	
	info->dict = shdict;
	info->stop = shstop;
	info->lookup = GetCurrentTimestamp();
	
	memcpy(info->dictFile, dictFile, strlen(dictFile) + 1);
	memcpy(info->affixFile, dictFile, strlen(affFile)+ 1);
	memcpy(info->stopFile, dictFile, strlen(stopFile) + 1);
	
}

Datum dispell_init(PG_FUNCTION_ARGS);
Datum dispell_lexize(PG_FUNCTION_ARGS);
Datum dispell_reset(PG_FUNCTION_ARGS);
Datum dispell_mem_available(PG_FUNCTION_ARGS);
Datum dispell_mem_used(PG_FUNCTION_ARGS);
Datum dispell_list_dicts(PG_FUNCTION_ARGS);
Datum dispell_list_stoplists(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(dispell_init);
PG_FUNCTION_INFO_V1(dispell_lexize);
PG_FUNCTION_INFO_V1(dispell_reset);
PG_FUNCTION_INFO_V1(dispell_mem_available);
PG_FUNCTION_INFO_V1(dispell_mem_used);
PG_FUNCTION_INFO_V1(dispell_list_dicts);
PG_FUNCTION_INFO_V1(dispell_list_stoplists);

Datum
dispell_reset(PG_FUNCTION_ARGS)
{
	LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);
	
	segment_info->dict = NULL;
	segment_info->stop = NULL;
	segment_info->lastReset = GetCurrentTimestamp();
	segment_info->firstfree = ((char*)segment_info) + MAXALIGN(sizeof(SegmentInfo));
	segment_info->available = max_ispell_mem_size - (int)(segment_info->firstfree - (char*)segment_info);
	
	memset(segment_info->firstfree, 0, segment_info->available);
	
	LWLockRelease(segment_info->lock);
	
	PG_RETURN_VOID();
}

Datum
dispell_mem_available(PG_FUNCTION_ARGS)
{
	int result = 0;
	LWLockAcquire(segment_info->lock, LW_SHARED);
	
	result = segment_info->available;
	
	LWLockRelease(segment_info->lock);
	
	PG_RETURN_INT32(result);
}

Datum
dispell_mem_used(PG_FUNCTION_ARGS)
{
	int result = 0;
	LWLockAcquire(segment_info->lock, LW_SHARED);
	
	result = max_ispell_mem_size - segment_info->available;
	
	LWLockRelease(segment_info->lock);
	
	PG_RETURN_INT32(result);
}

Datum
dispell_init(PG_FUNCTION_ARGS)
{
	List		*dictoptions = (List *) PG_GETARG_POINTER(0);
	char		*dictFile = NULL, *affFile = NULL, *stopFile = NULL;
	bool		affloaded = false,
				dictloaded = false,
				stoploaded = false;
	ListCell   *l;
	
	/* this is the result passed to dispell_lexize */
	DictInfo * info = (DictInfo *)palloc0(sizeof(DictInfo));

	foreach(l, dictoptions)
	{
		DefElem    *defel = (DefElem *) lfirst(l);
		
		if (pg_strcasecmp(defel->defname, "DictFile") == 0)
		{
			if (dictloaded)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("multiple DictFile parameters")));
			dictFile = defGetString(defel);
			dictloaded = true;
		}
		else if (pg_strcasecmp(defel->defname, "AffFile") == 0)
		{
			if (affloaded)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("multiple AffFile parameters")));
			affFile = defGetString(defel);
			affloaded = true;
		}
		else if (pg_strcasecmp(defel->defname, "StopWords") == 0)
		{
			if (stoploaded)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("multiple StopWords parameters")));
			stopFile = defGetString(defel);
			stoploaded = true;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("unrecognized Ispell parameter: \"%s\"",
							defel->defname)));
		}
	}
	
	if (!affloaded)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("missing AffFile parameter")));
	}
	else if (! dictloaded)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("missing DictFile parameter")));
	}

	/* search if the dictionary is already initialized */
	LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);
	
	init_shared_dict(info, dictFile, affFile, stopFile);
	
	LWLockRelease(segment_info->lock);

	PG_RETURN_POINTER(info);
}

Datum
dispell_lexize(PG_FUNCTION_ARGS)
{
	DictInfo * info = (DictInfo *) PG_GETARG_POINTER(0);
	char	   *in = (char *) PG_GETARG_POINTER(1);
	int32		len = PG_GETARG_INT32(2);
	char	   *txt;
	TSLexeme   *res;
	TSLexeme   *ptr,
			   *cptr;

	if (len <= 0)
		PG_RETURN_POINTER(NULL);

	txt = lowerstr_with_len(in, len);

	/* need to lock the segment in shared mode */
	LWLockAcquire(segment_info->lock, LW_SHARED);
	
	/* do we need to reinit the dictionary? was the dict reset since the lookup */
	if (timestamp_cmp_internal(info->lookup, segment_info->lastReset) < 0) {
		
		/* relock in exclusive mode */
		LWLockRelease(segment_info->lock);
		LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);
		
		elog(INFO, "reinitializing shared dict (segment reset)");
		
		init_shared_dict(info, info->dictFile, info->affixFile, info->stopFile);
	}
	
	res = SharedNINormalizeWord(info->dict, txt);

	/* nothing found :-( */
	if (res == NULL) {
		LWLockRelease(segment_info->lock);
		PG_RETURN_POINTER(NULL);
	}

	ptr = cptr = res;
	while (ptr->lexeme)
	{
		if (searchstoplist(&info->stop->list, ptr->lexeme))
		{
			pfree(ptr->lexeme);
			ptr->lexeme = NULL;
			ptr++;
		}
		else
		{
			memcpy(cptr, ptr, sizeof(TSLexeme));
			cptr++;
			ptr++;
		}
	}
	cptr->lexeme = NULL;
	
	LWLockRelease(segment_info->lock);

	PG_RETURN_POINTER(res);
}

static
char * shalloc(int bytes) {
	
	char * result;
	bytes = MAXALIGN(bytes);
	
	/* This shouldn't really happen, as the init_shared_dict checks the size
	 * prior to copy. So let's just throw error here, as something went
	 * obviously wrong. */
	if (bytes > segment_info->available) {
		elog(ERROR, "the shared segment (shared ispell) is too small");
	}
	
	result = segment_info->firstfree;
	segment_info->firstfree += bytes;
	segment_info->available -= bytes;
	
	memset(result, 0, bytes);
	
	return result;
	
}

static
SPNode * copySPNode(SPNode * node) {
	int i;
	
	SPNode * copy = NULL;
	
	if (node == NULL) {
		return NULL;
	}
	
	copy = (SPNode*)shalloc(offsetof(SPNode,data) + sizeof(SPNodeData) * node->length);
	memcpy(copy, node, offsetof(SPNode,data) + sizeof(SPNodeData) * node->length);
		
	for (i = 0; i < node->length; i++) {
		copy->data[i].node = copySPNode(node->data[i].node);
	}
	
	return copy;
}

static
int sizeSPNode(SPNode * node) {
	
	int i;
	int size = 0;
	
	if (node == NULL) {
		return 0;
	}
	
	size = MAXALIGN(offsetof(SPNode,data) + sizeof(SPNodeData) * node->length);
		
	for (i = 0; i < node->length; i++) {
		size += sizeSPNode(node->data[i].node);
	}
	
	return size;
}

static
char * shstrcpy(char * str) {
	char * tmp = shalloc(strlen(str)+1);
	memcpy(tmp, str, strlen(str)+1);
	return tmp;
}

static
RegisNode * copyRegisNode(RegisNode * node) {

	RegisNode * copy = (RegisNode *)shalloc(offsetof(RegisNode, data) + node->len);
	
	memcpy(copy, node, offsetof(RegisNode, data) + node->len);
	
	if (node->next != NULL) {
		copy->next = copyRegisNode(node->next);
	}

	return copy;
}

static
int sizeRegisNode(RegisNode * node) {
	
	int size = MAXALIGN(offsetof(RegisNode, data) + node->len);
	
	if (node->next != NULL) {
		size += sizeRegisNode(node->next);
	}

	return size;
}

static
char * copyRegexGuts(char * guts) {
	return NULL;
}

static
AFFIX * copyAffix(AFFIX * affix) {

	AFFIX * copy = (AFFIX*)shalloc(sizeof(AFFIX));
	
	memcpy(copy, affix, sizeof(AFFIX));
	
	copy->find = shstrcpy(affix->find);
	copy->repl = shstrcpy(affix->repl);
	
	if (affix->isregis) {
		copy->reg.regis.node = copyRegisNode(affix->reg.regis.node);
	} else if (! affix->issimple) {
		
		/*FIXME Need to copy the regex_t properly. But would a plain copy be
		 *      safe tu use by multiple processes at the same time? */
		elog(ERROR, "This extension can't handle regex_t affixes yet.");
		
	}
	
	return copy;

}

static
int sizeAffix(AFFIX * affix) {

	int size = MAXALIGN(sizeof(AFFIX));
	
	size += MAXALIGN(strlen(affix->find)+1);
	size += MAXALIGN(strlen(affix->repl)+1);
	
	if (affix->isregis) {
		size += sizeRegisNode(affix->reg.regis.node);
	} else if (! affix->issimple) {
		
		/*FIXME Need to copy the regex_t properly. But would a plain copy be
		 *      safe tu use by multiple processes at the same time? */
		elog(ERROR, "This extension can't handle regex_t affixes yet.");
		
	}
	
	return size;

}

static
AffixNode * copyAffixNode(AffixNode * node) {
	
	int i, j;
	AffixNode * copy = NULL;
	
	if (node == NULL) {
		return NULL;
	}
	
	copy = (AffixNode *)shalloc(offsetof(AffixNode,data) + sizeof(AffixNodeData) * node->length);
	memcpy(copy, node, offsetof(AffixNode,data) + sizeof(AffixNodeData) * node->length);
	
	for (i = 0; i < node->length; i++) {
		
		copy->data[i].node = copyAffixNode(node->data[i].node);
		
		copy->data[i].val = node->data[i].val;
		copy->data[i].naff = node->data[i].naff;
		copy->data[i].aff = (AFFIX**)shalloc(sizeof(AFFIX*) * node->data[i].naff);
					
		for (j = 0; j < node->data[i].naff; j++) {
			copy->data[i].aff[j] = copyAffix(node->data[i].aff[j]);
		}
	}
	
	return copy;
}

static
int sizeAffixNode(AffixNode * node) {
	
	int i, j;
	int size = 0;
	
	if (node == NULL) {
		return 0;
	}
	
	size = MAXALIGN(offsetof(AffixNode,data) + sizeof(AffixNodeData) * node->length);
	
	for (i = 0; i < node->length; i++) {
		
		size += sizeAffixNode(node->data[i].node);
		size += MAXALIGN(sizeof(AFFIX*) * node->data[i].naff);
					
		for (j = 0; j < node->data[i].naff; j++) {
			size += sizeAffix(node->data[i].aff[j]);
		}
	}
	
	return size;
}

static
SharedStopList * copyStopList(StopList * list, char * stopFile, int size) {
	
	int i;
	SharedStopList * copy = (SharedStopList *)shalloc(sizeof(SharedStopList));
	
	copy->list.len = list->len;
	copy->list.stop = (char**)shalloc(sizeof(char*) * list->len);
	copy->stopFile = shstrcpy(stopFile);
	copy->nbytes = size;
	
	for (i = 0; i < list->len; i++) {
		copy->list.stop[i] = shstrcpy(list->stop[i]);
	}
	
	return copy;
}

static
int sizeStopList(StopList * list, char * stopFile) {
	
	int i;
	int size = MAXALIGN(sizeof(SharedStopList));
	
	size += MAXALIGN(sizeof(char*) * list->len);
	size += MAXALIGN(strlen(stopFile) + 1);
	
	for (i = 0; i < list->len; i++) {
		size += MAXALIGN(strlen(list->stop[i]) + 1);
	}
	
	return size;
}

static
int countCMPDAffixes(CMPDAffix * affixes) {
	
	/* there's at least one affix */
	int count = 1;
	CMPDAffix * ptr = affixes;
	
	/* the last one is marked with (affix == NULL) */
	while (ptr->affix)
	{
		ptr++;
		count++;
	}
	
	return count;
	
}

static
SharedIspellDict * copyIspellDict(IspellDict * dict, char * dictFile, char * affixFile, int size, int words) {
	
	int i, cnt;

	SharedIspellDict * copy = (SharedIspellDict*)shalloc(sizeof(SharedIspellDict));
	
	copy->dictFile = shalloc(strlen(dictFile)+1);
	copy->affixFile = shalloc(strlen(affixFile)+1);
	
	strcpy(copy->dictFile, dictFile);
	strcpy(copy->affixFile, affixFile);

	copy->naffixes = dict->naffixes;
	
	copy->Affix = (AFFIX*)shalloc(sizeof(AFFIX) * dict->naffixes);

	copy->Suffix = copyAffixNode(dict->Suffix);
	copy->Prefix = copyAffixNode(dict->Prefix);

	copy->Dictionary = copySPNode(dict->Dictionary);

	/* copy affix data */
	copy->nAffixData = dict->nAffixData;	
	copy->AffixData = (char**)shalloc(sizeof(char*) * dict->nAffixData);
	for (i = 0; i < copy->nAffixData; i++) {
		copy->AffixData[i] = shstrcpy(dict->AffixData[i]);
	}

	/* copy compound affixes (there's at least one) */
	cnt = countCMPDAffixes(dict->CompoundAffix);
	copy->CompoundAffix = (CMPDAffix*)shalloc(sizeof(CMPDAffix) * cnt);
	memcpy(copy->CompoundAffix, dict->CompoundAffix, sizeof(CMPDAffix) * cnt);

	memcpy(copy->flagval, dict->flagval, 255);
	copy->usecompound = dict->usecompound;
	
	copy->nbytes = size;
	copy->nwords = words;
	
	return copy;
	
}

static
int sizeIspellDict(IspellDict * dict, char * dictFile, char * affixFile) {
	
	int i;
	int size = MAXALIGN(sizeof(SharedIspellDict));
	
	size += MAXALIGN(strlen(dictFile)+1);
	size += MAXALIGN(strlen(affixFile)+1);
	
	size += MAXALIGN(sizeof(AFFIX) * dict->naffixes);
	
	size += MAXALIGN(sizeAffixNode(dict->Suffix));
	size += MAXALIGN(sizeAffixNode(dict->Prefix));
	
	size += sizeSPNode(dict->Dictionary);
	
	/* copy affix data */
	size += MAXALIGN(sizeof(char*) * dict->nAffixData);
	for (i = 0; i < dict->nAffixData; i++) {
		size += MAXALIGN(sizeof(char) * strlen(dict->AffixData[i]) + 1);
	}
	
	/* copy compound affixes (there's at least one) */
	size += MAXALIGN(sizeof(CMPDAffix) * countCMPDAffixes(dict->CompoundAffix));
	
	return size;
	
}

Datum
dispell_list_dicts(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc        tupdesc;
	AttInMetadata   *attinmeta;
	SharedIspellDict * dict;
		
	/* init on the first call */
	if (SRF_IS_FIRSTCALL()) {
		
		MemoryContext oldcontext;
		
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* get a shared lock and then the first dictionary */
		LWLockAcquire(segment_info->lock, LW_SHARED);
		funcctx->user_fctx = segment_info->dict;
		
		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		/*
		 * generate attribute metadata needed later to produce tuples from raw
		 * C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;
		funcctx->tuple_desc = tupdesc;
		
		/* switch back to the old context */
		MemoryContextSwitchTo(oldcontext);
		
	}
	
	/* init the context */
	funcctx = SRF_PERCALL_SETUP();
	
	/* check if we have more data */
	if (funcctx->user_fctx != NULL)
	{
		HeapTuple	tuple;
		Datum		result;
		Datum		values[5];
		bool		nulls[5];
		
		text		*dictname, *affname;
		
		dict = (SharedIspellDict*)funcctx->user_fctx;
		funcctx->user_fctx = dict->next;
		
		memset(nulls, 0, sizeof(nulls));
		
		dictname = (text *) palloc(strlen(dict->dictFile) + VARHDRSZ);
		affname  = (text *) palloc(strlen(dict->affixFile) + VARHDRSZ);
		
		SET_VARSIZE(dictname, strlen(dict->dictFile) + VARHDRSZ);
		SET_VARSIZE(affname,  strlen(dict->affixFile)  + VARHDRSZ);
		
		strcpy(VARDATA(dictname), dict->dictFile);
		strcpy(VARDATA(affname),  dict->affixFile);
		
		values[0] = PointerGetDatum(dictname);
		values[1] = PointerGetDatum(affname);
		values[2] = UInt32GetDatum(dict->nwords);
		values[3] = UInt32GetDatum(dict->naffixes);
		values[4] = UInt32GetDatum(dict->nbytes);
	   
		/* Build and return the tuple. */
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		
		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		/* Here we want to return another item: */
		SRF_RETURN_NEXT(funcctx, result);
		
	}
	else
	{
		/* release the lock */
		LWLockRelease(segment_info->lock);
		
		/* Here we are done returning items and just need to clean up: */
		SRF_RETURN_DONE(funcctx);
	}

}

Datum
dispell_list_stoplists(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TupleDesc        tupdesc;
	AttInMetadata   *attinmeta;
	SharedStopList  *stoplist;
		
	/* init on the first call */
	if (SRF_IS_FIRSTCALL()) {
		
		MemoryContext oldcontext;
		
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* get a shared lock and then the first stop list */
		LWLockAcquire(segment_info->lock, LW_SHARED);
		funcctx->user_fctx = segment_info->stop;
		
		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		/*
		 * generate attribute metadata needed later to produce tuples from raw
		 * C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;
		funcctx->tuple_desc = tupdesc;
		
		/* switch back to the old context */
		MemoryContextSwitchTo(oldcontext);
		
	}
	
	/* init the context */
	funcctx = SRF_PERCALL_SETUP();
	
	/* check if we have more data */
	if (funcctx->user_fctx != NULL)
	{
		HeapTuple	tuple;
		Datum		result;
		Datum		values[3];
		bool		nulls[3];
		
		text		*stopname;
		
		stoplist = (SharedStopList*)funcctx->user_fctx;
		funcctx->user_fctx = stoplist->next;
		
		memset(nulls, 0, sizeof(nulls));
		
		stopname = (text *) palloc(strlen(stoplist->stopFile) + VARHDRSZ);
		
		SET_VARSIZE(stopname, strlen(stoplist->stopFile) + VARHDRSZ);
		
		strcpy(VARDATA(stopname), stoplist->stopFile);
		
		values[0] = PointerGetDatum(stopname);
		values[1] = UInt32GetDatum(stoplist->list.len);
		values[2] = UInt32GetDatum(stoplist->nbytes);
	   
		/* Build and return the tuple. */
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		
		/* make the tuple into a datum */
		result = HeapTupleGetDatum(tuple);

		/* Here we want to return another item: */
		SRF_RETURN_NEXT(funcctx, result);
		
	}
	else
	{
		/* release the lock */
		LWLockRelease(segment_info->lock);
		
		/* Here we are done returning items and just need to clean up: */
		SRF_RETURN_DONE(funcctx);
	}

}
