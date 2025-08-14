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

       You’re right—let’s make this surgical and precise.

Where to paste

In your DataLineageAnalyzer._simple_ai_variable_extraction method, paste the block right after:

progress_bar.empty()
status_text.empty()

and just before:

st.write("🧭 **Normalising variables (deterministic & language-aware)...**")

Paste this exact block

        # === PATCH: usage-only input detection (Python; catches RemainingTerm, etc.) ===
        # If a column is only ever READ (df['col'] / df.col) and never ASSIGNED on LHS,
        # treat it as an INPUT variable even if the AI missed it.

        # 1) Find Python DataFrame column *reads*
        py_reads_sq = re.findall(
            r"\b([A-Za-z_]\w*)\s*\[\s*['\"]([A-Za-z_]\w*)['\"]\s*\]",
            self.compiled_code
        )
        py_reads_dot = re.findall(
            r"\b([A-Za-z_]\w*)\.(?!read_csv|read_excel|to_csv|to_excel|assign|merge|join|groupby|apply|loc|iloc|values|shape|columns)([A-Za-z_]\w*)\b",
            self.compiled_code
        )

        # 2) Heuristic: DF-like object names
        df_like_names = {
            n for n, _ in (py_reads_sq + py_reads_dot)
            if re.match(r"^(df|data|tbl|tmp|dftmp|result|results)\w*$", n, flags=re.IGNORECASE)
        }

        usage_read_cols = set()
        for n, c in py_reads_sq:
            if n in df_like_names:
                usage_read_cols.add(c)
        for n, c in py_reads_dot:
            if n in df_like_names:
                usage_read_cols.add(c)

        # 3) Find Python DataFrame column *assignments* (LHS)
        lhs_assign_sq = re.findall(
            r"\b[A-Za-z_]\w*\s*\[\s*['\"]([A-Za-z_]\w*)['\"]\s*\]\s*=",
            self.compiled_code
        )
        lhs_assign_dot = re.findall(
            r"\b[A-Za-z_]\w*\.([A-Za-z_]\w*)\s*=",
            self.compiled_code
        )
        lhs_assign_via_assign = re.findall(
            r"\.assign\s*\(\s*([A-Za-z_]\w*)\s*=",
            self.compiled_code
        )

        assigned_cols = set(lhs_assign_sq) | set(lhs_assign_dot) | set(lhs_assign_via_assign)

        # 4) “Pure usage” columns = read but never assigned → definitely inputs
        pure_usage_inputs = sorted(usage_read_cols - assigned_cols)

        # 5) Merge into AI result before normalization
        all_ai['input_variables'].extend(pure_usage_inputs)
        # === END PATCH ===

That’s it.
This makes RemainingTerm (and similar columns that are used but never assigned) show up as Input consistently, without touching your downstream logic.