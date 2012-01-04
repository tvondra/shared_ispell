/*-------------------------------------------------------------------------
 *
 * spell.c
 * 
 * Normalizing word with ISpell (in shared segment)
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Copyright (c) 2011, Tomas Vondra
 *
 * IDENTIFICATION
 *  src/spell.c (a slightly modified copy of src/backend/tsearch/spell.c)
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "spell.h"

#define MAX_NORM 1024
#define MAXNORMLEN 256

#define GETWCHAR(W,L,N,T) ( ((const uint8*)(W))[ ((T)==FF_PREFIX) ? (N) : ( (L) - 1 - (N) ) ] )

static int
FindWord(SharedIspellDict *Conf, const char *word, int affixflag, int flag)
{
	SPNode	   *node = Conf->Dictionary;
	SPNodeData *StopLow,
			   *StopHigh,
			   *StopMiddle;
	const uint8 *ptr = (const uint8 *) word;

	flag &= FF_DICTFLAGMASK;

	while (node && *ptr)
	{
		StopLow = node->data;
		StopHigh = node->data + node->length;
		while (StopLow < StopHigh)
		{
			StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);
			if (StopMiddle->val == *ptr)
			{
				if (*(ptr + 1) == '\0' && StopMiddle->isword)
				{
					if (flag == 0)
					{
						if (StopMiddle->compoundflag & FF_COMPOUNDONLY)
							return 0;
					}
					else if ((flag & StopMiddle->compoundflag) == 0)
						return 0;

					if ((affixflag == 0) || (strchr(Conf->AffixData[StopMiddle->affix], affixflag) != NULL))
						return 1;
				}
				node = StopMiddle->node;
				ptr++;
				break;
			}
			else if (StopMiddle->val < *ptr)
				StopLow = StopMiddle + 1;
			else
				StopHigh = StopMiddle;
		}
		if (StopLow >= StopHigh)
			break;
	}
	return 0;
}

static AffixNodeData *
FindAffixes(AffixNode *node, const char *word, int wrdlen, int *level, int type)
{
	AffixNodeData *StopLow,
			   *StopHigh,
			   *StopMiddle;
	uint8 symbol;

	if (node->isvoid)
	{							/* search void affixes */
		if (node->data->naff)
			return node->data;
		node = node->data->node;
	}

	while (node && *level < wrdlen)
	{
		StopLow = node->data;
		StopHigh = node->data + node->length;
		while (StopLow < StopHigh)
		{
			StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);
			symbol = GETWCHAR(word, wrdlen, *level, type);

			if (StopMiddle->val == symbol)
			{
				(*level)++;
				if (StopMiddle->naff)
					return StopMiddle;
				node = StopMiddle->node;
				break;
			}
			else if (StopMiddle->val < symbol)
				StopLow = StopMiddle + 1;
			else
				StopHigh = StopMiddle;
		}
		if (StopLow >= StopHigh)
			break;
	}
	return NULL;
}

static char *
CheckAffix(const char *word, size_t len, AFFIX *Affix, int flagflags, char *newword, int *baselen)
{
	/*
	 * Check compound allow flags
	 */

	if (flagflags == 0)
	{
		if (Affix->flagflags & FF_COMPOUNDONLY)
			return NULL;
	}
	else if (flagflags & FF_COMPOUNDBEGIN)
	{
		if (Affix->flagflags & FF_COMPOUNDFORBIDFLAG)
			return NULL;
		if ((Affix->flagflags & FF_COMPOUNDBEGIN) == 0)
			if (Affix->type == FF_SUFFIX)
				return NULL;
	}
	else if (flagflags & FF_COMPOUNDMIDDLE)
	{
		if ((Affix->flagflags & FF_COMPOUNDMIDDLE) == 0 ||
			(Affix->flagflags & FF_COMPOUNDFORBIDFLAG))
			return NULL;
	}
	else if (flagflags & FF_COMPOUNDLAST)
	{
		if (Affix->flagflags & FF_COMPOUNDFORBIDFLAG)
			return NULL;
		if ((Affix->flagflags & FF_COMPOUNDLAST) == 0)
			if (Affix->type == FF_PREFIX)
				return NULL;
	}

	/*
	 * make replace pattern of affix
	 */
	if (Affix->type == FF_SUFFIX)
	{
		strcpy(newword, word);
		strcpy(newword + len - Affix->replen, Affix->find);
		if (baselen)			/* store length of non-changed part of word */
			*baselen = len - Affix->replen;
	}
	else
	{
		/*
		 * if prefix is a all non-chaged part's length then all word contains
		 * only prefix and suffix, so out
		 */
		if (baselen && *baselen + strlen(Affix->find) <= Affix->replen)
			return NULL;
		strcpy(newword, Affix->find);
		strcat(newword, word + Affix->replen);
	}

	/*
	 * check resulting word
	 */
	if (Affix->issimple)
		return newword;
	else if (Affix->isregis)
	{
		if (RS_execute(&(Affix->reg.regis), newword))
			return newword;
	}
	else
	{
		int			err;
		pg_wchar   *data;
		size_t		data_len;
		int			newword_len;

		/* Convert data string to wide characters */
		newword_len = strlen(newword);
		data = (pg_wchar *) palloc((newword_len + 1) * sizeof(pg_wchar));
		data_len = pg_mb2wchar_with_len(newword, data, newword_len);

		if (!(err = pg_regexec(&(Affix->reg.regex), data, data_len, 0, NULL, 0, NULL, 0)))
		{
			pfree(data);
			return newword;
		}
		pfree(data);
	}

	return NULL;
}

static int
addToResult(char **forms, char **cur, char *word)
{
	if (cur - forms >= MAX_NORM - 1)
		return 0;
	if (forms == cur || strcmp(word, *(cur - 1)) != 0)
	{
		*cur = pstrdup(word);
		*(cur + 1) = NULL;
		return 1;
	}

	return 0;
}

static char **
NormalizeSubWord(SharedIspellDict *Conf, char *word, int flag)
{
	AffixNodeData *suffix = NULL,
			   *prefix = NULL;
	int			slevel = 0,
				plevel = 0;
	int			wrdlen = strlen(word),
				swrdlen;
	char	  **forms;
	char	  **cur;
	char		newword[2 * MAXNORMLEN] = "";
	char		pnewword[2 * MAXNORMLEN] = "";
	AffixNode  *snode = Conf->Suffix,
			   *pnode;
	int			i,
				j;

	if (wrdlen > MAXNORMLEN)
		return NULL;
	cur = forms = (char **) palloc(MAX_NORM * sizeof(char *));
	*cur = NULL;


	/* Check that the word itself is normal form */
	if (FindWord(Conf, word, 0, flag))
	{
		*cur = pstrdup(word);
		cur++;
		*cur = NULL;
	}

	/* Find all other NORMAL forms of the 'word' (check only prefix) */
	pnode = Conf->Prefix;
	plevel = 0;
	while (pnode)
	{
		prefix = FindAffixes(pnode, word, wrdlen, &plevel, FF_PREFIX);
		if (!prefix)
			break;
		for (j = 0; j < prefix->naff; j++)
		{
			if (CheckAffix(word, wrdlen, prefix->aff[j], flag, newword, NULL))
			{
				/* prefix success */
				if (FindWord(Conf, newword, prefix->aff[j]->flag, flag))
					cur += addToResult(forms, cur, newword);
			}
		}
		pnode = prefix->node;
	}

	/*
	 * Find all other NORMAL forms of the 'word' (check suffix and then
	 * prefix)
	 */
	while (snode)
	{
		int			baselen = 0;

		/* find possible suffix */
		suffix = FindAffixes(snode, word, wrdlen, &slevel, FF_SUFFIX);
		if (!suffix)
			break;
		/* foreach suffix check affix */
		for (i = 0; i < suffix->naff; i++)
		{
			if (CheckAffix(word, wrdlen, suffix->aff[i], flag, newword, &baselen))
			{
				/* suffix success */
				if (FindWord(Conf, newword, suffix->aff[i]->flag, flag))
					cur += addToResult(forms, cur, newword);

				/* now we will look changed word with prefixes */
				pnode = Conf->Prefix;
				plevel = 0;
				swrdlen = strlen(newword);
				while (pnode)
				{
					prefix = FindAffixes(pnode, newword, swrdlen, &plevel, FF_PREFIX);
					if (!prefix)
						break;
					for (j = 0; j < prefix->naff; j++)
					{
						if (CheckAffix(newword, swrdlen, prefix->aff[j], flag, pnewword, &baselen))
						{
							/* prefix success */
							int			ff = (prefix->aff[j]->flagflags & suffix->aff[i]->flagflags & FF_CROSSPRODUCT) ?
							0 : prefix->aff[j]->flag;

							if (FindWord(Conf, pnewword, ff, flag))
								cur += addToResult(forms, cur, pnewword);
						}
					}
					pnode = prefix->node;
				}
			}
		}

		snode = suffix->node;
	}

	if (cur == forms)
	{
		pfree(forms);
		return (NULL);
	}
	return (forms);
}

typedef struct SplitVar
{
	int			nstem;
	int			lenstem;
	char	  **stem;
	struct SplitVar *next;
} SplitVar;

static int
CheckCompoundAffixes(CMPDAffix **ptr, char *word, int len, bool CheckInPlace)
{
	bool		issuffix;

	if (CheckInPlace)
	{
		while ((*ptr)->affix)
		{
			if (len > (*ptr)->len && strncmp((*ptr)->affix, word, (*ptr)->len) == 0)
			{
				len = (*ptr)->len;
				issuffix = (*ptr)->issuffix;
				(*ptr)++;
				return (issuffix) ? len : 0;
			}
			(*ptr)++;
		}
	}
	else
	{
		char	   *affbegin;

		while ((*ptr)->affix)
		{
			if (len > (*ptr)->len && (affbegin = strstr(word, (*ptr)->affix)) != NULL)
			{
				len = (*ptr)->len + (affbegin - word);
				issuffix = (*ptr)->issuffix;
				(*ptr)++;
				return (issuffix) ? len : 0;
			}
			(*ptr)++;
		}
	}
	return -1;
}

static SplitVar *
CopyVar(SplitVar *s, int makedup)
{
	SplitVar   *v = (SplitVar *) palloc(sizeof(SplitVar));

	v->next = NULL;
	if (s)
	{
		int			i;

		v->lenstem = s->lenstem;
		v->stem = (char **) palloc(sizeof(char *) * v->lenstem);
		v->nstem = s->nstem;
		for (i = 0; i < s->nstem; i++)
			v->stem[i] = (makedup) ? pstrdup(s->stem[i]) : s->stem[i];
	}
	else
	{
		v->lenstem = 16;
		v->stem = (char **) palloc(sizeof(char *) * v->lenstem);
		v->nstem = 0;
	}
	return v;
}

static void
AddStem(SplitVar *v, char *word)
{
	if (v->nstem >= v->lenstem)
	{
		v->lenstem *= 2;
		v->stem = (char **) repalloc(v->stem, sizeof(char *) * v->lenstem);
	}

	v->stem[v->nstem] = word;
	v->nstem++;
}

static SplitVar *
SplitToVariants(SharedIspellDict *Conf, SPNode *snode, SplitVar *orig, char *word, int wordlen, int startpos, int minpos)
{
	SplitVar   *var = NULL;
	SPNodeData *StopLow,
			   *StopHigh,
			   *StopMiddle = NULL;
	SPNode	   *node = (snode) ? snode : Conf->Dictionary;
	int			level = (snode) ? minpos : startpos;	/* recursive
														 * minpos==level */
	int			lenaff;
	CMPDAffix  *caff;
	char	   *notprobed;
	int			compoundflag = 0;

	notprobed = (char *) palloc(wordlen);
	memset(notprobed, 1, wordlen);
	var = CopyVar(orig, 1);

	while (level < wordlen)
	{
		/* find word with epenthetic or/and compound affix */
		caff = Conf->CompoundAffix;
		while (level > startpos && (lenaff = CheckCompoundAffixes(&caff, word + level, wordlen - level, (node) ? true : false)) >= 0)
		{
			/*
			 * there is one of compound affixes, so check word for existings
			 */
			char		buf[MAXNORMLEN];
			char	  **subres;

			lenaff = level - startpos + lenaff;

			if (!notprobed[startpos + lenaff - 1])
				continue;

			if (level + lenaff - 1 <= minpos)
				continue;

			if (lenaff >= MAXNORMLEN)
				continue;		/* skip too big value */
			if (lenaff > 0)
				memcpy(buf, word + startpos, lenaff);
			buf[lenaff] = '\0';

			if (level == 0)
				compoundflag = FF_COMPOUNDBEGIN;
			else if (level == wordlen - 1)
				compoundflag = FF_COMPOUNDLAST;
			else
				compoundflag = FF_COMPOUNDMIDDLE;
			subres = NormalizeSubWord(Conf, buf, compoundflag);
			if (subres)
			{
				/* Yes, it was a word from dictionary */
				SplitVar   *new = CopyVar(var, 0);
				SplitVar   *ptr = var;
				char	  **sptr = subres;

				notprobed[startpos + lenaff - 1] = 0;

				while (*sptr)
				{
					AddStem(new, *sptr);
					sptr++;
				}
				pfree(subres);

				while (ptr->next)
					ptr = ptr->next;
				ptr->next = SplitToVariants(Conf, NULL, new, word, wordlen, startpos + lenaff, startpos + lenaff);

				pfree(new->stem);
				pfree(new);
			}
		}

		if (!node)
			break;

		StopLow = node->data;
		StopHigh = node->data + node->length;
		while (StopLow < StopHigh)
		{
			StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);
			if (StopMiddle->val == ((uint8 *) (word))[level])
				break;
			else if (StopMiddle->val < ((uint8 *) (word))[level])
				StopLow = StopMiddle + 1;
			else
				StopHigh = StopMiddle;
		}

		if (StopLow < StopHigh)
		{
			if (level == FF_COMPOUNDBEGIN)
				compoundflag = FF_COMPOUNDBEGIN;
			else if (level == wordlen - 1)
				compoundflag = FF_COMPOUNDLAST;
			else
				compoundflag = FF_COMPOUNDMIDDLE;

			/* find infinitive */
			if (StopMiddle->isword &&
				(StopMiddle->compoundflag & compoundflag) &&
				notprobed[level])
			{
				/* ok, we found full compoundallowed word */
				if (level > minpos)
				{
					/* and its length more than minimal */
					if (wordlen == level + 1)
					{
						/* well, it was last word */
						AddStem(var, pnstrdup(word + startpos, wordlen - startpos));
						pfree(notprobed);
						return var;
					}
					else
					{
						/* then we will search more big word at the same point */
						SplitVar   *ptr = var;

						while (ptr->next)
							ptr = ptr->next;
						ptr->next = SplitToVariants(Conf, node, var, word, wordlen, startpos, level);
						/* we can find next word */
						level++;
						AddStem(var, pnstrdup(word + startpos, level - startpos));
						node = Conf->Dictionary;
						startpos = level;
						continue;
					}
				}
			}
			node = StopMiddle->node;
		}
		else
			node = NULL;
		level++;
	}

	AddStem(var, pnstrdup(word + startpos, wordlen - startpos));
	pfree(notprobed);
	return var;
}

static void
addNorm(TSLexeme **lres, TSLexeme **lcur, char *word, int flags, uint16 NVariant)
{
	if (*lres == NULL)
		*lcur = *lres = (TSLexeme *) palloc(MAX_NORM * sizeof(TSLexeme));

	if (*lcur - *lres < MAX_NORM - 1)
	{
		(*lcur)->lexeme = word;
		(*lcur)->flags = flags;
		(*lcur)->nvariant = NVariant;
		(*lcur)++;
		(*lcur)->lexeme = NULL;
	}
}

TSLexeme *
SharedNINormalizeWord(SharedIspellDict *Conf, char *word)
{
	char	  **res;
	TSLexeme   *lcur = NULL,
			   *lres = NULL;
	uint16		NVariant = 1;

	res = NormalizeSubWord(Conf, word, 0);

	if (res)
	{
		char	  **ptr = res;

		while (*ptr && (lcur - lres) < MAX_NORM)
		{
			addNorm(&lres, &lcur, *ptr, 0, NVariant++);
			ptr++;
		}
		pfree(res);
	}

	if (Conf->usecompound)
	{
		int			wordlen = strlen(word);
		SplitVar   *ptr,
				   *var = SplitToVariants(Conf, NULL, NULL, word, wordlen, 0, -1);
		int			i;

		while (var)
		{
			if (var->nstem > 1)
			{
				char	  **subres = NormalizeSubWord(Conf, var->stem[var->nstem - 1], FF_COMPOUNDLAST);

				if (subres)
				{
					char	  **subptr = subres;

					while (*subptr)
					{
						for (i = 0; i < var->nstem - 1; i++)
						{
							addNorm(&lres, &lcur, (subptr == subres) ? var->stem[i] : pstrdup(var->stem[i]), 0, NVariant);
						}

						addNorm(&lres, &lcur, *subptr, 0, NVariant);
						subptr++;
						NVariant++;
					}

					pfree(subres);
					var->stem[0] = NULL;
					pfree(var->stem[var->nstem - 1]);
				}
			}

			for (i = 0; i < var->nstem && var->stem[i]; i++)
				pfree(var->stem[i]);
			ptr = var->next;
			pfree(var->stem);
			pfree(var);
			var = ptr;
		}
	}

	return lres;
}
