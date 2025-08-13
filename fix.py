Here’s the simple table, Master — first column is the code snippet I added, second column is what that code does in plain English.

Code I added	What it does
python ident = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$') 	Makes sure only proper variable names (letters, numbers, underscores) are kept.
python if v in ds_ban: continue 	Removes any variable name that is actually a dataset name.
python params.add(m.group(1)) 	Finds macro variables or constants and marks them as parameters, not columns.
```python m = re.match(r’^([A-Za-z_][A-Za-z0-9_]*)_(\d+	i)$’, name) ```
python fam = f"{m.group(1)}_*" 	Groups looped variables into one family label (e.g., var_*).
python created_vars = (created_vars - members) | {fam} 	Replaces all looped variables in “created” with their one family name.
python all_vars = sorted(input_vars | created_vars | interm_vars, key=str.lower) 	Sorts all variables alphabetically so the order is always the same.
python info["category"] = fixed_category(v, info.get("category", "unknown")) 	Overrides AI’s guess for variable category with the stable one from above.
python if ds_vals & input_ds: info["dataset"] = "N/A" 	If AI links a created/intermediate var to an input dataset, clear that link.
python cleaned[v] = info 	Keeps only variables that are valid and not datasets when building the final table.
python input_datasets = _dedupe_keep(input_datasets) 	Removes duplicates and junk words from dataset lists before returning them.

Nice—thanks for pasting the full file. You’ve already got most of Patch A and C in there ✅.
What’s still missing (and causing the flip-flops) is the deterministic category override inside generate_detailed_variable_lineage (Patch B). Below is a tiny, exact drop-in that you paste right after batch_results = future.result() in that method’s as_completed(...) loop.

⸻

1) Paste this block (Patch B) exactly here

Anchor in your file:

with ThreadPoolExecutor(max_workers=1) as executor:
    future_to_batch = {
        executor.submit(process_lineage_batch, batch): batch['batch_id']
        for batch in variable_batches
    }
    completed_batches = 0

    for future in as_completed(future_to_batch):
        try:
            batch_results = future.result()
            # >>> PASTE HERE <<<

Code to paste:

            # === DETERMINISTIC CATEGORY OVERRIDE (stabilise Created/Intermediate/Input) ===
            cats = getattr(self, "_dl_static_categories", None)
            ds_info = getattr(self, "_dl_datasets", {}) or {}
            input_ds = set(ds_info.get("input", []))

            def _fixed_cat(vname: str, ai_cat: str):
                # If we have the static buckets from _simple_ai_variable_extraction, trust them
                if not cats:
                    return ai_cat or "unknown"
                if vname in cats.get("inputs", []):
                    return "input"
                if vname in cats.get("intermediate", []):
                    return "intermediate"
                if vname in cats.get("created", []):
                    return "created"
                # Family label inheritance (e.g., Average_EAD_*):
                for fam_label, members in cats.get("families", {}).items():
                    # vname may be the family label itself or a member the AI returned
                    if vname == fam_label:
                        # If any known created member exists, treat family as created
                        if set(members) & set(cats.get("created", [])):
                            return "created"
                        if set(members) & set(cats.get("intermediate", [])):
                            return "intermediate"
                    if vname in members:
                        # Member should inherit the family bucket
                        if fam_label in cats.get("created", []) or (set(members) & set(cats.get("created", []))):
                            return "created"
                        if fam_label in cats.get("intermediate", []) or (set(members) & set(cats.get("intermediate", []))):
                            return "intermediate"
                return ai_cat or "unknown"

            # Apply fixes to each result in this batch before merging
            for v, info in list(batch_results.items()):
                # normalise variable name (keep exactly what AI returned as key)
                ai_cat = (info or {}).get("category", "unknown")
                final_cat = _fixed_cat(v, ai_cat)
                info["category"] = final_cat

                # Never attach an INPUT dataset to a created/intermediate variable
                if final_cat in ("created", "intermediate"):
                    ds_val = info.get("dataset", [])
                    ds_vals = set(ds_val if isinstance(ds_val, list) else [ds_val])
                    if ds_vals & input_ds:
                        info["dataset"] = "N/A"
            # === END OVERRIDE ===

Then keep your current lines:

            detailed_lineage.update(batch_results)
            completed_batches += 1
            progress = completed_batches / len(variable_batches)
            progress_bar.progress(progress)
            status_text.text(f"Analyzed {completed_batches}/{len(variable_batches)} lineage batches")


⸻

2) Two tiny cleanups you already hinted at (optional but recommended)

These aren’t required for stability, but they remove noise:

A. In process_lineage_batch(...) you compute all_code twice and then loop over selected_variables (global) instead of the current variables batch.
It doesn’t break anything, but it’s wasted work and confusing.

Change:

all_code = ""
for file in code_files:
    content = file.read().decode('utf-8')
    all_code += f"\n{content}\n"
    file.seek(0)

detailed_lineage = {}

for variable in selected_variables:  # <-- this should be 'variables'
    ...

to:

all_code = ""
for file in code_files:
    content = file.read().decode('utf-8')
    all_code += f"\n{content}\n"
    file.seek(0)

detailed_lineage = {}

for variable in variables:  # use the current batch's variables
    ...

(And you can delete the earlier all_code construction that includes file headers; you only need one.)

B. Sorting for display stability.
You already sort categories earlier. If you want the final table to be stable too, right before you build lineage_table, do:

for sr_no, (variable, info) in enumerate(sorted(detailed_lineage.items(), key=lambda kv: kv[0].lower()), 1):
    ...


⸻

Why this fixes your SAS/Python pair
	•	Families by pattern are grouped in _simple_ai_variable_extraction (you already added it).
	•	Dropped families (SAS drop X1-X&max / Python df.drop(columns=[...])) are treated as Intermediate (you’ve got that in the post-pass).
	•	Final, per-variable override (this patch) forces each lineage item’s category to the deterministic bucket from the post-pass, before merge—so Created/Intermediate can’t flip between runs, even if the AI’s JSON wobbles.
	•	Input datasets can no longer be attached to created/intermediate vars.

# Optional: pass deterministic hints if available
category_hints = getattr(self, "_dl_static_categories", None)
dataset_hints  = getattr(self, "_dl_datasets", None)

lineage_prompt = f"""
YOU ARE A PRECISE DATA-LINEAGE ENGINE. RETURN ONLY JSON.

GOAL
Trace the lineage for each target variable with stable categories (input|intermediate|created),
correct dataset assignment, exact calculation, and source variables.

TARGET VARIABLES
{variables_str}

CODE (ALL FILES)
{all_code}

STRICT DEFINITIONS
- INPUT: a column that already exists in an input dataset/table/file (not created by code).
- INTERMEDIATE: a column produced by a transformation step that does not represent the final output,
  including looped arrays and temporary columns.
- CREATED: a business-facing column intended to be used downstream or saved/exported.

SAS RULES
- Treat SAS %let, %global as PARAMETERS (not variables).
- DATA step assignments (e.g., new = old * 2;) produce variables in the active dataset.
- PROC SQL SELECT expressions create variables in the target table.
- Variables dropped later are still variables; array-like loop outputs (name_1, name_2, …) are INTERMEDIATE unless saved as final.
- Imports: PROC IMPORT / SET / FROM mark INPUT datasets; EXPORT / DATA final / CREATE TABLE mark OUTPUT.

PYTHON RULES
- Only treat DataFrame column ops as variables:
  ✓ df['x'] = ...
  ✓ df = df.assign(x=...)
  ✗ x = 0.05 (PARAMETER, not a variable)
- Reads (pd.read_csv/pd.read_excel) mark INPUT datasets; writes (to_csv/to_excel) mark OUTPUT.
- Columns created and later dropped (especially loop arrays like name_1, name_2, …) are INTERMEDIATE.

FAMILY PATTERN (LOOPS)
- If a variable name matches ^([A-Za-z_][A-Za-z0-9_]*)_(\\d+)$, treat the whole group as one FAMILY (e.g., base_*).
- Family members are INTERMEDIATE unless explicitly kept as final or exported.

DATASET FIELD (MUST FOLLOW)
- If category == "input": dataset MUST be the INPUT dataset name (e.g., file/table read).
- If category in {"created","intermediate"}: dataset MUST be the OUTPUT dataset name if saved/exported;
  otherwise "N/A".
- Exactly one dataset name per variable (use "N/A" if not determinable).

PARAMETERS / CONSTANTS (NOT VARIABLES)
- Do NOT include macro variables (%let NAME=...), globals, scalars like RATE=0.05, file paths, counters (i,j,k).

DETERMINISTIC HINTS (IF PROVIDED)
{json.dumps(category_hints or {}, indent=2)}
{json.dumps(dataset_hints or {}, indent=2)}

USE HINTS STRICTLY:
- If a name is listed under parameters: never classify it as a variable.
- If a name appears under created/intermediate/input hints: prefer that category unless code clearly contradicts it.
- If a member of a FAMILY is intermediate: the whole FAMILY is intermediate unless exported.

OUTPUT JSON SCHEMA (ONLY THIS):
{{
  "lineage_results": [
    {{
      "variable": "variable_name",
      "category": "input|intermediate|created",
      "source_variables": ["source1","source2"],
      "dataset": ["dataset_name_or_N/A"],
      "calculation": "exact formula or logic (copy expressions if possible)",
      "description": "plain-English explanation",
      "business_purpose": "why this variable exists",
      "code_location": "FileName:Line(s) if clear, else 'N/A'",
      "detailed_steps": ["step 1","step 2","step 3"],
      "family": "base_* if variable belongs to a loop family, else 'N/A'"
    }}
  ]
}}

REQUIREMENTS
- Cover EVERY target variable—no omissions.
- Use FAMILY for looped arrays (pattern above).
- Respect dataset rule and categories strictly.
- Prefer exact expressions from code for "calculation".
- Never include parameters/macros/paths in variables.

RETURN ONLY THE JSON. NO EXTRA TEXT.
"""