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

    def _simple_ai_variable_extraction(self, code, variables):
        # existing AI call + result merging logic...
        all_ai = {
            "input_variables": [],
            "intermediate_variables": [],
            "created_variables": []
        }

        # ============================================
        # AI extraction per batch (existing code)
        # ============================================
        futures = []
        for chunk in self._split_code_for_batches(code):
            futures.append(self.executor.submit(self._ask_ai_lineage, chunk, variables))

        for future in futures:
            batch_results = future.result()
            for k in all_ai.keys():
                all_ai[k].extend(batch_results.get(k, []))

        # ============================================
        # --- NEW: detect Python DataFrame column usages ---
        # This catches usage-only inputs like RemainingTerm
        # ============================================
        import re
        py_col_square = re.findall(
            r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\[\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]\s*\]",
            code
        )
        py_col_dot = re.findall(
            r"\b([A-Za-z_][A-Za-z0-9_]*)\.(?!read_csv|read_excel|to_csv|to_excel|assign|merge|join|groupby|apply|loc|iloc|values|shape|columns)([A-Za-z_][A-Za-z0-9_]*)\b",
            code
        )

        # Only keep likely DataFrame objects
        _df_like = {
            n for n, _ in py_col_square + py_col_dot
            if re.match(r"^(df|data|tbl|tmp|dftmp|results)\w*$", n, re.IGNORECASE)
        }

        usage_only_inputs = set()
        for n, col in py_col_square:
            if n in _df_like:
                usage_only_inputs.add(col)
        for n, col in py_col_dot:
            if n in _df_like:
                usage_only_inputs.add(col)

        # Merge detected usage-only inputs into AI's list
        all_ai["input_variables"].extend(sorted(usage_only_inputs))
        # ============================================
        # --- END NEW ---
        # ============================================

        # deterministic clean + family grouping (your existing Patch B/C logic)
        all_ai = self._clean_ai_results(all_ai)

        return all_ai