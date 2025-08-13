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

lineage_prompt = f"""
You are a precise data-lineage engine. Your task is to return JSON with detailed variable lineage information.

Target variables:
{variables_str}

Code:
{all_code}

Definitions:
- INPUT: A column that already exists in an input dataset/table/file and is read into the code (not newly created).
- INTERMEDIATE: A column created within the process but not part of the final business output, including temporary loop arrays and helper calculations.
- CREATED: A final business-facing column intended for downstream use or export.

Language-specific rules:
SAS:
- Treat %let and %global as parameters, not variables.
- DATA step assignments create variables in the active dataset.
- PROC SQL SELECT expressions create variables in the target table.
- Imports (PROC IMPORT / SET / FROM) mark input datasets; exports (PROC EXPORT / DATA final / CREATE TABLE) mark outputs.
- Variables dropped later are still variables; looped arrays like var_1, var_2… are intermediate unless kept as final.

Python:
- Treat only DataFrame column operations as variables (df['x'] = ..., df = df.assign(...)).
- Scalar assignments (x = 0.05) are parameters, not variables.
- Reads (pd.read_csv, pd.read_excel) mark input datasets; writes (to_csv, to_excel) mark outputs.
- Columns created and later dropped, especially looped arrays, are intermediate unless explicitly retained in the final output.

Family pattern for looped variables:
- If a variable name matches ^([A-Za-z_][A-Za-z0-9_]*)_(\\d+)$, group the whole set as one "family" (e.g., Average_EAD_*).
- Family members share the same category, usually intermediate unless exported.

Dataset rules:
- If category is "input", dataset must be the name of the input dataset.
- If category is "created" or "intermediate", dataset is the output dataset name if saved/exported, otherwise "N/A".
- Only one dataset name per variable.

Parameters and constants:
- Do not classify parameters, constants, counters (i, j, k), or file paths as variables.

Output JSON schema (return only JSON):
{{
  "lineage_results": [
    {{
      "variable": "variable_name",
      "category": "input|intermediate|created",
      "source_variables": ["source1", "source2"],
      "dataset": ["dataset_name" or "N/A"],
      "calculation": "exact expression or logic from the code",
      "description": "plain English explanation",
      "business_purpose": "why this variable exists",
      "code_location": "FileName:Line(s) if clear, else 'N/A'",
      "detailed_steps": ["step 1", "step 2", "step 3"],
      "family": "base_* if part of a loop family, else 'N/A'"
    }}
  ]
}}

Requirements:
- Cover every target variable listed above.
- Apply the same classification rules for SAS and Python.
- Respect dataset naming rules.
- Use exact code expressions for "calculation" where possible.
- Do not include any non-variables.
- Return only valid JSON.
"""