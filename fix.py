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

Yes Master, there’s a much simpler option if you don’t want a whole new helper function.

We can do it inside the same function without separate parsing, just by checking dependencies that the AI already extracted.

⸻

Minimal change approach

Inside generate_full_data_lineage(...), after you get cleaned but before you build the table:

# --- SIMPLE RECLASSIFY: If a variable is used by another, mark it as intermediate ---
for var, info in cleaned.items():
    current_cat = str(info.get('category', '')).lower().strip()

    # Skip if already input
    if current_cat == 'input':
        continue

    # Check if var appears in any other variable's dependencies
    is_used_elsewhere = any(
        var in details.get('dependencies', [])
        for other_var, details in cleaned.items()
        if other_var != var
    )

    if is_used_elsewhere:
        info['category'] = 'intermediate'
    else:
        info['category'] = 'created'
# --- END RECLASSIFY ---


⸻

Why this works
	•	It reuses the dependencies that your AI extraction already produces — no regex parsing of the raw code.
	•	If a variable is used in another’s dependencies, it’s Intermediate.
	•	If it’s assigned but never used elsewhere, it’s Created.
	•	Inputs remain Input.

This will automatically mark Pmts_to_Miss as Intermediate in both SAS and Python lineage.

⸻

If you want, I can show you exactly where to paste this in your current Data_lineage.py so you don’t need to hunt for the spot.
Do you want me to do that?