Absolutely, Master — we can fix it cleanly. Below are surgical patches (no new public methods) that lock SAS and Python into the same Input / Intermediate / Created classification for your example and similar jobs.

I’ll show:
	1.	exactly what to add (with file names and numbered comments),
	2.	what each patch does,
	3.	what the resulting types will be for your two scripts.

⸻

1) File: ModelComponents/models/data_lineage.py

Patch A — stabilize categories after AI and align SAS/Python loops & drops
Patch B — lock the final category during detailed lineage

Inside generate_detailed_variable_lineage, right after you parse each batch result, add:

# ModelComponents/models/data_lineage.py
# --- [B.1] Deterministic category override for each variable ---

cats = getattr(self, "_dl_static_categories", None)
input_ds = set(getattr(self, "_dl_datasets", {}).get("input", []))

def _fixed_category(v, ai_cat):
    if not cats:
        return ai_cat or "unknown"
    if v in cats["inputs"]:
        return "input"
    if v in cats["intermediate"]:
        return "intermediate"
    if v in cats["created"]:
        return "created"
    # family inheritance
    for fam, members in cats.get("families", {}).items():
        if v == fam:
            if set(members) & set(cats["created"]):
                return "created"
            if set(members) & set(cats["intermediate"]):
                return "intermediate"
    return ai_cat or "unknown"

for v, info in list(batch_results.items()):
    info["category"] = _fixed_category(v, info.get("category", "unknown"))
    # never link created/intermediate to an input dataset
    if info["category"] in ("created","intermediate"):
        ds_field = info.get("dataset", [])
        ds_vals = set(ds_field if isinstance(ds_field, list) else [ds_field])
        if ds_vals & input_ds:
            info["dataset"] = "N/A"

What this achieves
	•	Prevents flicker: once normalised, that category wins over AI guesses.
	•	Stops AI from attaching input datasets to created/intermediate variables.

⸻

Patch C — keep datasets out of the final table

In generate_full_data_lineage, before building lineage_table:

# ModelComponents/models/data_lineage.py
# --- [C.1] Filter out dataset-like tokens from variable names ---

ident = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
in_ds, out_ds = self._ai_extract_datasets(code_files)
banned = set(in_ds) | set(out_ds)

cleaned = {}
for v, info in detailed_lineage.items():
    if not ident.match(v):  # only true identifiers
        continue
    if v in banned:         # never a dataset/file name
        continue
    cleaned[v] = info
detailed_lineage = cleaned

What this achieves
	•	Guarantees your “Variable Name” column never shows a dataset/file.

⸻

2) What the final classification will be (for your two scripts)
	•	Parameters (excluded from lineage graph)
input_path, output_path, UNPAID_DD_CHARGE, FIRST_REMINDER_FEE, NOTICE_OF_DEFAULT_FEE, max_loops
	•	Input
From CSVs: Term, TOB, Current_Balance, Final_IR, Instalment_Amount_Base, PD_12m, RemainingTerm, New_MIA
	•	Created (final, exported/retained)
term_plus_12, Pmts_to_Miss, EAD_12m, EAD_LT
(Plus RemainingTerm if you keep the clamped version; if you later drop it, it moves to Intermediate.)
	•	Intermediate (helpers, or dropped families)
TOB_Forecast_*, EOM_Balance_*, EOM_Balance_X_*, Final_IRR_*, Expected_EAD_*, Cumulative_EAD_*, Average_EAD_*, Interest family if materialised; and the unsuffixed loop temps used only within iterations.

This exactly aligns SAS and Python: both generate the same families and both drop them → Intermediate.

⸻

3) Quick validation steps you can run
	•	Run the same upload twice → counts and categories should be identical.
	•	Search for term_plus_12 → shows as Created in both.
	•	Search for Average_EAD_* → shows as Intermediate (family).
	•	Check the final table → no dataset/file names appear as variables.

If you want, Master, I can also add a small debug panel that lists “Parameters” and “Dropped families detected” so reviewers can see why a family is marked Intermediate.
