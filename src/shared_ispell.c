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

#include "libpq/md5.h"

#include "spell.h"

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
	/* the shared segment (info and data) */
	SharedIspellDict * dict;
} SegmentInfo;

/* These are used to allocate data within shared segment */
static SegmentInfo * segment_info = NULL;

static char * shalloc(int bytes);
static void copyIspellDict(IspellDict * dict, SharedIspellDict * copy);

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
        
        elog(DEBUG1, "shared memory segment (shared ispell) successfully created");
        
    }
    
    LWLockRelease(AddinShmemInitLock);
    
}

static
SharedIspellDict * get_shared_dict(char * words, char * affixes) {
	
	SharedIspellDict * dict = segment_info->dict;
	
	while (dict != NULL) {
		if ((strcmp(dict->dictFile, words) == 0) && (strcmp(dict->affixFile, affixes) == 0)) {
			return dict;
		}
		dict = dict->next;
	}
	
	return NULL;
}

Datum dispell_init(PG_FUNCTION_ARGS);
Datum dispell_lexize(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(dispell_init);
PG_FUNCTION_INFO_V1(dispell_lexize);

StopList	stoplist;

Datum
dispell_init(PG_FUNCTION_ARGS)
{
	List		*dictoptions = (List *) PG_GETARG_POINTER(0);
	char		*dictFile = NULL, *affFile = NULL, *stopFile = NULL;
	bool		affloaded = false,
				dictloaded = false,
				stoploaded = false;
	ListCell   *l;
	
	IspellDict * dict;
	SharedIspellDict * shdict;

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

	/* search if the dictionary is already initialized */
	LWLockAcquire(segment_info->lock, LW_EXCLUSIVE);
	
	shdict = get_shared_dict(dictFile, affFile);
	
	/* init if needed */
	if (shdict == NULL) {
		
		if (affloaded && dictloaded)
		{
			 dict = (IspellDict *)palloc(sizeof(IspellDict));
		
			SharedNIStartBuild(dict);
			
			SharedNIImportDictionary(dict,
							get_tsearch_config_filename(dictFile, "dict"));
			SharedNIImportAffixes(dict,
							get_tsearch_config_filename(affFile, "affix"));
			
			/* FIXME use the stoplist */
			/* readstoplist(stopFile, &stoplist, lowerstr); */
			
			SharedNISortDictionary(dict);
			SharedNISortAffixes(dict);
		}
		else if (!affloaded)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("missing AffFile parameter")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("missing DictFile parameter")));
		}

		SharedNIFinishBuild(dict);
		
		shdict = (SharedIspellDict*)shalloc(sizeof(SharedIspellDict));
		
		shdict->dictFile = shalloc(strlen(dictFile)+1);
		shdict->affixFile = shalloc(strlen(affFile)+1);
		
		strcpy(shdict->dictFile, dictFile);
		strcpy(shdict->affixFile, affFile);
	
		copyIspellDict(dict, shdict);
		
		shdict->next = segment_info->dict;
		segment_info->dict = shdict;
		
		elog(LOG, "shared ispell init done, remaining %d B", segment_info->available);
		
	}
	
	LWLockRelease(segment_info->lock);

	PG_RETURN_POINTER(shdict);
}

Datum
dispell_lexize(PG_FUNCTION_ARGS)
{
	SharedIspellDict *d = (SharedIspellDict *) PG_GETARG_POINTER(0);
	char	   *in = (char *) PG_GETARG_POINTER(1);
	int32		len = PG_GETARG_INT32(2);
	char	   *txt;
	TSLexeme   *res;
	TSLexeme   *ptr,
			   *cptr;

	if (len <= 0)
		PG_RETURN_POINTER(NULL);

	txt = lowerstr_with_len(in, len);
	res = SharedNINormalizeWord(d, txt);

	if (res == NULL)
		PG_RETURN_POINTER(NULL);

	ptr = cptr = res;
	while (ptr->lexeme)
	{
		if (searchstoplist(&stoplist, ptr->lexeme))
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

	PG_RETURN_POINTER(res);
}

static
char * shalloc(int bytes) {
	
	char * result;
	bytes = MAXALIGN(bytes);
	
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
	} else {
		// FIXME handle the regex_t properly (copy the strings etc)
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
	copy->isvoid = node->isvoid;
	copy->length = node->length;
	memcpy(copy, node, offsetof(SPNode,data) + sizeof(SPNodeData) * node->length);
	
	for (i = 0; i < node->length; i++) {
		
		copy->data[i].node = copyAffixNode(node->data[i].node);
		
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
void copyIspellDict(IspellDict * dict, SharedIspellDict * copy) {
	
	int i;

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

	/* copy compound affixes */
	/* FIXME How to copy this without the cmpaffixes? If we can get rid of this field, we
	 *       could get rid of the local IspellDict copy. */
	copy->CompoundAffix = (CMPDAffix*)shalloc(sizeof(CMPDAffix) * dict->cmpaffixes);
	memcpy(copy->CompoundAffix, dict->CompoundAffix, sizeof(CMPDAffix) * dict->cmpaffixes);

	memcpy(copy->flagval, dict->flagval, 255);
	copy->usecompound = dict->usecompound;
	
}
