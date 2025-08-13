Master, got it. If both “move-after” and “placeholder” approaches still only show the button after you toggle modes, you are almost certainly hitting Streamlit’s rerun timing quirk. The safest fix is to persist the HTML into session state, then immediately force a rerun, and render the download button unconditionally on the next pass if the artifact exists. Two small, surgical edits solve it.

Below are compact patches for both the main lineage viz and the manual Excel viz. They are drop‑in and will make the button appear immediately after generation.

⸻

Patch 1 — Force a rerun right after saving the HTML

File: app.py

A) Main lineage visualization

Find the block where you create and store the viz:

viz_html = visualizer.create_visualization_with_modes(full_lineage_data, view_mode)

if viz_html:
    if 'lineage_viz_html_dict' not in st.session_state:
        st.session_state['lineage_viz_html_dict'] = {}

    st.session_state['lineage_viz_html_dict'][view_mode] = viz_html
    st.session_state['lineage_viz_mode'] = view_mode

    st.success(f"✅ {view_mode.title()} visualization created successfully!")

Replace with:

viz_html = visualizer.create_visualization_with_modes(full_lineage_data, view_mode)

if viz_html:
    if 'lineage_viz_html_dict' not in st.session_state:
        st.session_state['lineage_viz_html_dict'] = {}

    st.session_state['lineage_viz_html_dict'][view_mode] = viz_html
    st.session_state['lineage_viz_mode'] = view_mode

    # 🔁 Force immediate UI rebuild so the download button appears right away
    st.rerun()

Rationale, Streamlit evaluates top to bottom. After a button click, you compute and set state, but the UI above was already drawn. Forcing a rerun right here guarantees the next pass sees the stored HTML and renders the download button immediately.

B) Manual Excel visualization

Do the same after storing manual_viz_html:

if manual_viz_html:
    if 'manual_viz_dict' not in st.session_state:
        st.session_state['manual_viz_dict'] = {}

    manual_viz_key = f"manual_viz_{manual_view_mode}"
    st.session_state['manual_viz_dict'][manual_viz_key] = manual_viz_html

    st.rerun()


⸻

Patch 2 — Always render the download button after the viz, using bytes

Render the button after the visualization section and read directly from session state. Also, feed bytes to data to avoid any edge cases with large HTML strings.

A) Main lineage download button

Place this after you display the viz or the placeholder, not inside the earlier column trio:

# 🔽 Download button rendered after potential creation/display
if st.session_state.get('lineage_viz_html_dict', {}).get(view_mode):
    viz_html = st.session_state['lineage_viz_html_dict'][view_mode]
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        label="📥 Download HTML",
        data=viz_html.encode("utf-8"),          # ← robust: send bytes
        file_name=f"lineage_{view_mode}_{timestamp}.html",
        mime="text/html",
        key=f"download_viz_{view_mode}_btn"
    )

B) Manual Excel download button

Place this after you display the manual viz:

manual_viz_key = f"manual_viz_{manual_view_mode}"
if st.session_state.get('manual_viz_dict', {}).get(manual_viz_key):
    manual_viz_html = st.session_state['manual_viz_dict'][manual_viz_key]
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        label="📥 Download HTML",
        data=manual_viz_html.encode("utf-8"),   # ← send bytes
        file_name=f"manual_lineage_{manual_view_mode}_{timestamp}.html",
        mime="text/html",
        key=f"download_manual_viz_{manual_view_mode}_btn"
    )


⸻

Optional hardening, avoids “column scope” surprises

If you still prefer the download button to sit in the same row as your controls, create a persistent placeholder at the top, then fill it later, and keep the forced st.rerun():

# Controls row
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    view_mode = st.selectbox(...)

with col2:
    create_viz_btn = st.button(...)

dl_placeholder = col3.empty()  # persistent slot for the download button

# After visualization display:
if st.session_state.get('lineage_viz_html_dict', {}).get(view_mode):
    viz_html = st.session_state['lineage_viz_html_dict'][view_mode]
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    with dl_placeholder:
        st.download_button(
            label="📥 Download HTML",
            data=viz_html.encode("utf-8"),
            file_name=f"lineage_{view_mode}_{timestamp}.html",
            mime="text/html",
            key=f"download_viz_{view_mode}_btn"
        )


⸻

Why your current behaviour happens
	•	Streamlit widgets are laid out before your long operation sets session state, so the first render pass has nothing to show.
	•	Even if you set session state later in the same run, that early part of the page is already built.
	•	Toggling modes forces a new rerun, at which point the condition is true and the button appears.

st.rerun() immediately after saving the artifact eliminates that “one extra interaction” requirement.

If you paste the two patches above exactly, your download buttons will appear right away, Master. If anything still feels sticky, send me the few lines around your create_visualization_with_modes calls and I will inline the exact diff on those lines.