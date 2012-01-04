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

#include "libpq/md5.h"

#include "spell.h"
#include "tsearch/dicts/spell.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
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
static SharedIspellDict * copyIspellDict(IspellDict * dict, char * dictFile, char * affixFile);
static SharedStopList * copyStopList(StopList * list, char * stopFile);

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
	
	SharedIspellDict * shdict;
	SharedStopList * shstop;
	
	IspellDict * dict;
	StopList	stoplist;
	
	/* FIXME Maybe we could treat the stop file separately, as it does not
	 * influence the dictionary. So the SharedIspellDict would track just
	 * dictionary and affixes, and the stop words would be kept somewhere
	 * else - either separately in the shared segment, or in local memory
	 * (the list is usually small and easy pro load) */
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
	
		shdict = copyIspellDict(dict, dictFile, affFile);
		
		shdict->next = segment_info->dict;
		segment_info->dict = shdict;
		
		elog(LOG, "shared ispell init done, remaining %ld B", segment_info->available);
		
	}
	
	/* stop words */
	shstop = get_shared_stop_list(stopFile);
	if ((shstop == NULL) && (stopFile != NULL)) {
		
		readstoplist(stopFile, &stoplist, lowerstr);
		shstop = copyStopList(&stoplist, stopFile);
		
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

PG_FUNCTION_INFO_V1(dispell_init);
PG_FUNCTION_INFO_V1(dispell_lexize);
PG_FUNCTION_INFO_V1(dispell_reset);
PG_FUNCTION_INFO_V1(dispell_mem_available);
PG_FUNCTION_INFO_V1(dispell_mem_used);

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
	
	if (bytes > segment_info->available) {
		/* FIXME this should not throw error, it should rather return NULL
		 * and reset the alloc info (this way the memory is wasted forever) */
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
AFFIX * copyAffix(AFFIX * affix) {

	AFFIX * copy = (AFFIX*)shalloc(sizeof(AFFIX));
	
	memcpy(copy, affix, sizeof(AFFIX));
	
	copy->find = shstrcpy(affix->find);
	copy->repl = shstrcpy(affix->repl);
	
	if (copy->isregis) {
		copy->reg.regis.node = copyRegisNode(copy->reg.regis.node);
	} else if (! copy->issimple) {
		// FIXME handle the regex_t properly (copy the strings etc)
		elog(WARNING, "skipping regex_t");
	}
	
	return copy;

}

static
AffixNode * copyAffixNode(AffixNode * node) {
	
	int i, j;
	AffixNode * copy = NULL;
	
	if (node == NULL) {
		return NULL;
	}
	
	copy = (AffixNode *)shalloc(offsetof(AffixNode,data) + sizeof(AffixNodeData) * node->length);
	memcpy(copy, node, offsetof(SPNode,data) + sizeof(SPNodeData) * node->length);
	
	for (i = 0; i < node->length; i++) {
		
		copy->data[i].node = copyAffixNode(node->data[i].node);
		
		copy->data[i].val = node->data[i].val;
		copy->data[i].naff = node->data[i].naff;
		copy->data[i].aff = (AFFIX**)shalloc(sizeof(AFFIX*) * node->data[i].naff);
		memset(copy->data[i].aff, 0, sizeof(AFFIX*) * node->data[i].naff);
					
		for (j = 0; j < node->data[i].naff; j++) {
			copy->data[i].aff[j] = copyAffix(node->data[i].aff[j]);
		}
	}
	
	return copy;
}

static
SharedStopList * copyStopList(StopList * list, char * stopFile) {
	
	int i;
	SharedStopList * copy = (SharedStopList *)shalloc(sizeof(SharedStopList));
	
	copy->list.len = list->len;
	copy->list.stop = (char**)shalloc(sizeof(char*) * list->len);
	copy->stopFile = (char*)shalloc(strlen(stopFile) + 1);
	memcpy(copy->stopFile, stopFile, strlen(stopFile) + 1);
	
	for (i = 0; i < list->len; i++) {
		copy->list.stop[i] = shalloc(strlen(list->stop[i]) + 1);
		memcpy(copy->list.stop[i], list->stop[i], strlen(list->stop[i]) + 1);
	}
	
	return copy;
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
SharedIspellDict * copyIspellDict(IspellDict * dict, char * dictFile, char * affixFile) {
	
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
		copy->AffixData[i] = (char*)shalloc(sizeof(char) * strlen(dict->AffixData[i]) + 1);
		strcpy(copy->AffixData[i], dict->AffixData[i]);
	}

	/* copy compound affixes (there's at least one) */
	cnt = countCMPDAffixes(dict->CompoundAffix);
	copy->CompoundAffix = (CMPDAffix*)shalloc(sizeof(CMPDAffix) * cnt);
	memcpy(copy->CompoundAffix, dict->CompoundAffix, sizeof(CMPDAffix) * cnt);

	memcpy(copy->flagval, dict->flagval, 255);
	copy->usecompound = dict->usecompound;
	
	return copy;
	
}
