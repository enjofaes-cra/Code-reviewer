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

If you want, I can now mark exactly where in your functions each of these lines slot in, so you can see the before/after flow with no guessing. Would you like me to do that, Master?