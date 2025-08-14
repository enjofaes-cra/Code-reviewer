You’re super close—the reason it still shows Created is just two tiny bugs in your reclassify step:
	1.	You’re checking details.get('dependencies', []), but your AI payload uses the key source_variables (not dependencies).
	2.	There’s a case mismatch: you refer to Pmts_to_miss in places, but the variable is Pmts_to_Miss. A case-insensitive map fixes that.

Below is a drop-in replacement for your current “SIMPLE RECLASSIFY” block. Paste it in generate_full_data_lineage, exactly where your current reclassify loop is (right after detailed_lineage = cleaned), replacing your whole block that starts with # --- SIMPLE RECLASSIFY...:

# --- SIMPLE RECLASSIFY (fixed: use source_variables + case-insensitive matching) ---

# Build a case-insensitive index of variables we actually know about
name_map = {k.lower(): k for k in cleaned.keys()}

# Reverse usage: for each var, who uses it?
uses_by: Dict[str, set] = {k: set() for k in cleaned.keys()}
for var, info in cleaned.items():
    for src in (info.get('source_variables') or []):
        # normalize source name to our known keys regardless of case
        src_key = name_map.get(str(src).strip().lower())
        if src_key:
            uses_by[src_key].add(var)

# Reclassify:
# - keep 'input' as input
# - if a var is used by any other var -> INTERMEDIATE
# - else -> CREATED
for var, info in cleaned.items():
    current_cat = str(info.get('category', '')).lower().strip()
    if current_cat == 'input':
        continue
    if uses_by.get(var):   # used by someone else
        info['category'] = 'intermediate'
    else:
        info['category'] = 'created'
# --- END SIMPLE RECLASSIFY ---

Why this fixes Pmts_to_Miss:
	•	If Pmts_to_Miss appears in other variables’ source_variables, the uses_by map will contain it → it is Intermediate by definition (“used as input by another variable”).
	•	The case-insensitive name_map means Pmts_to_Miss and pmts_to_miss get matched correctly.
	•	No manual override; it’s purely graph-based.

If you also want to prefer “Intermediate” even when a variable is both used elsewhere and written to an output dataset, the above already does that (usage wins). If you’d rather treat “written to final output” as Created only when not used anywhere else, keep it as is.

If you still see it labeled Created after this change, two quick checks:
	•	Print a tiny debug right after the loop to verify usage was detected:

st.write({v: len(s) for v, s in uses_by.items() if s})


	•	Confirm your AI response actually returns source_variables containing Pmts_to_Miss (exact tokens). If it returns slightly different names, we can add a fuzzy match, but usually the case-insensitive map is enough.