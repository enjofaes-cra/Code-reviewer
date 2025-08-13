Master, you are running into a classic Streamlit “order of execution” issue.

Right now you render the Download HTML button before you potentially create and stash the visualization in st.session_state. On the run where the user clicks Create Visualization, your code sets the session value after the “show the download button” check has already executed, so the button appears only on the next rerun.

Fix it by rendering the download button after you’ve possibly created the viz, or by using a placeholder you fill later.

Below are two clean, minimal patches. Either one will make the download button show up immediately.

⸻

Option A — Move the download button below creation

What to change
	1.	Main lineage viz controls: remove the early “with col3: download button” block, then render the download button after the if create_viz_btn: block and after the “Display the visualization” section.
	2.	Manual Excel viz controls: same change for the manual section.

Patch snippets

File: app.py
Replace the main lineage controls block around the download button:

# [A1] OLD – remove this early download button block
with col3:
    if st.session_state.get('lineage_viz_html_dict', {}).get(view_mode):
        viz_html = st.session_state['lineage_viz_html_dict'][view_mode]
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="📥 Download HTML",
            data=viz_html,
            file_name=f"lineage_{view_mode}_{timestamp}.html",
            mime="text/html",
            key=f"download_viz_{view_mode}_btn",
            help=f"Download {view_mode} visualization as HTML file"
        )

Then, add this block after the visualization creation and display logic (right after you either show the viz or the placeholder text):

# [A2] NEW – show the download button AFTER we may have created the viz
if st.session_state.get('lineage_viz_html_dict', {}).get(view_mode):
    viz_html = st.session_state['lineage_viz_html_dict'][view_mode]
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        label="📥 Download HTML",
        data=viz_html,
        file_name=f"lineage_{view_mode}_{timestamp}.html",
        mime="text/html",
        key=f"download_viz_{view_mode}_btn",
        help=f"Download {view_mode} visualization as HTML file"
    )

Do the same for the Manual Excel section:

File: app.py
Remove the early block:

# [A3] OLD – remove early manual download button
with col3:
    manual_viz_key = f"manual_viz_{manual_view_mode}"
    if st.session_state.get('manual_viz_dict', {}).get(manual_viz_key):
        manual_viz_html = st.session_state['manual_viz_dict'][manual_viz_key]
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="📥 Download HTML",
            data=manual_viz_html,
            file_name=f"manual_lineage_{manual_view_mode}_{timestamp}.html",
            mime="text/html",
            key=f"download_manual_viz_{manual_view_mode}_btn"
        )

Add the new block after you display the manual visualization (or its placeholder):

# [A4] NEW – render manual download button at the end
manual_viz_key = f"manual_viz_{manual_view_mode}"
if st.session_state.get('manual_viz_dict', {}).get(manual_viz_key):
    manual_viz_html = st.session_state['manual_viz_dict'][manual_viz_key]
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        label="📥 Download HTML",
        data=manual_viz_html,
        file_name=f"manual_lineage_{manual_view_mode}_{timestamp}.html",
        mime="text/html",
        key=f"download_manual_viz_{manual_view_mode}_btn"
    )


⸻

Option B — Use placeholders you fill later

This keeps your current layout but guarantees immediate visibility by creating an empty slot first, then filling it after creation.

File: app.py

In the controls row:

# [B1] Create a placeholder for the download button
dl_placeholder = col3.empty()

Replace the original early download-button block with the placeholder line above. Then, after the viz is created or loaded:

# [B2] Fill the placeholder after potential creation
if st.session_state.get('lineage_viz_html_dict', {}).get(view_mode):
    viz_html = st.session_state['lineage_viz_html_dict'][view_mode]
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    with dl_placeholder:
        st.download_button(
            label="📥 Download HTML",
            data=viz_html,
            file_name=f"lineage_{view_mode}_{timestamp}.html",
            mime="text/html",
            key=f"download_viz_{view_mode}_btn",
            help=f"Download {view_mode} visualization as HTML file"
        )

Repeat the same placeholder pattern for the manual section.

⸻

Why this works

Streamlit executes top to bottom on every rerun. Buttons trigger a rerun where their value is True. By moving or deferring the rendering of the download button until after you have potentially written to st.session_state, you meet the condition in the same run, so the button appears immediately.

If you prefer to keep your existing order without placeholders, a more brute-force alternative is calling st.rerun() right after you store the HTML in session state. However, that causes an extra rerun and is usually less smooth than Options A or B.

If you want, Master, I can produce a cleaned diff of your app.py with either option applied end to end.