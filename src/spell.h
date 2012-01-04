/*-------------------------------------------------------------------------
 *
 * spell.h
 *
 * Declarations for ISpell dictionary
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 *
 * src/include/tsearch/dicts/spell.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __SHARED_SPELL_H__
#define __SHARED_SPELL_H__

#include "regex/regex.h"
#include "tsearch/dicts/regis.h"
#include "tsearch/ts_public.h"
#include "storage/lwlock.h"
#include "tsearch/dicts/spell.h"

typedef struct SharedIspellDict
{
	
	/* this is used for selecting the dictionary */
	char *	dictFile;
	char *  affixFile;
	
	/* next dictionary in the chain */
	struct SharedIspellDict * next;
	
	/* the copied fields */
	int			naffixes;
	AFFIX	   *Affix;

	AffixNode  *Suffix;
	AffixNode  *Prefix;

	SPNode	   *Dictionary;
	char	  **AffixData;
	int			lenAffixData;
	int			nAffixData;

	CMPDAffix  * CompoundAffix;

	unsigned char flagval[256];
	bool		usecompound;
	
} SharedIspellDict;

typedef struct SharedStopList
{
	
	char *  stopFile;
	
	StopList list;
	struct SharedStopList * next;
	
} SharedStopList;

TSLexeme *SharedNINormalizeWord(SharedIspellDict *Conf, char *word);

#endif