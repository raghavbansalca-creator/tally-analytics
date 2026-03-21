"""
Seven Labs Vision — Setup & Connection
Connect to TallyPrime and sync data.
"""

import streamlit as st
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

st.set_page_config(page_title="Setup — Seven Labs Vision", page_icon="⚙️", layout="wide")

# ── HEADER ──────────────────────────────────────────────────────────────────

st.markdown("""
<div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            padding: 2rem; border-radius: 12px; margin-bottom: 1.5rem; text-align: center;">
    <h1 style="color: white; margin: 0;">⚙️ Seven Labs Vision — Setup</h1>
    <p style="color: #a0a0c0; margin: 0.5rem 0 0 0;">Connect to TallyPrime and sync your financial data</p>
</div>
""", unsafe_allow_html=True)

# ── CONNECTION SETTINGS ─────────────────────────────────────────────────────

col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("### 🔗 TallyPrime Connection")
    st.markdown("""
    **Prerequisites:**
    1. TallyPrime must be running on your computer (or network)
    2. A company must be loaded in TallyPrime
    3. Tally's XML server must be enabled on port 9000

    *To enable: In TallyPrime → F1 (Help) → Settings → Connectivity → Set "Enable ODBC Server" to Yes*
    """)

    # Connection form
    tally_host = st.text_input("Tally Host / IP Address",
                                value=st.session_state.get("tally_host", "localhost"),
                                help="Use 'localhost' if Tally is on this computer, or enter the IP address")
    tally_port = st.number_input("Tally Port", value=9000, min_value=1, max_value=65535,
                                  help="Default TallyPrime port is 9000")

    st.session_state.tally_host = tally_host
    st.session_state.tally_port = int(tally_port)

with col2:
    st.markdown("### 📊 Current Status")

    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tally_data.db")

    if os.path.exists(db_path):
        import sqlite3
        try:
            conn = sqlite3.connect(db_path)
            # Get company name
            company = "Unknown"
            try:
                row = conn.execute("SELECT company_name FROM sync_meta ORDER BY synced_at DESC LIMIT 1").fetchone()
                if row:
                    company = row[0]
            except:
                pass

            # Get counts
            counts = {}
            for table in ["mst_group", "mst_ledger", "mst_stock_item", "trn_voucher", "trn_accounting"]:
                try:
                    c = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    counts[table] = c
                except:
                    counts[table] = 0
            conn.close()

            st.success(f"**Database loaded**")
            st.metric("Company", company)
            st.metric("Ledgers", f"{counts.get('mst_ledger', 0):,}")
            st.metric("Vouchers", f"{counts.get('trn_voucher', 0):,}")
            st.metric("Entries", f"{counts.get('trn_accounting', 0):,}")
        except Exception as e:
            st.warning(f"Database exists but error reading: {e}")
    else:
        st.warning("**No data yet** — Connect to Tally and sync to get started!")

# ── ACTION BUTTONS ──────────────────────────────────────────────────────────

st.markdown("---")

col_test, col_sync = st.columns(2)

with col_test:
    if st.button("🔍 Test Connection", use_container_width=True, type="secondary"):
        try:
            from tally_sync import test_connection
            with st.spinner(f"Connecting to {tally_host}:{int(tally_port)}..."):
                result = test_connection(tally_host, int(tally_port))

            if result["success"]:
                st.success(f"✅ Connected! Company: **{result['company']}**")
                st.session_state.tally_connected = True
                st.session_state.tally_company = result["company"]
            else:
                st.error(f"❌ Connection failed: {result['error']}")
                st.markdown("""
                **Troubleshooting:**
                - Is TallyPrime running?
                - Is a company loaded?
                - Is the IP address correct?
                - Is port 9000 open? (Check: F1 → Settings → Connectivity)
                """)
        except ImportError:
            st.error("tally_sync module not found. Please ensure tally_sync.py is in the app directory.")
        except Exception as e:
            st.error(f"Error: {e}")

with col_sync:
    if st.button("🔄 Sync All Data", use_container_width=True, type="primary"):
        try:
            from tally_sync import sync_all

            progress_bar = st.progress(0)
            status_text = st.empty()
            detail_text = st.empty()

            progress_state = {"done": 0}
            total_steps = 10  # approximate

            def progress_callback(step_name, current, total):
                progress_state["done"] += 1
                pct = min(progress_state["done"] / total_steps, 0.99)
                progress_bar.progress(pct)
                status_text.markdown(f"**{step_name}**")
                if current and total:
                    detail_text.markdown(f"*{current}/{total} records*")
                elif current:
                    detail_text.markdown(f"*{current} records extracted*")

            status_text.markdown("**Starting sync...**")
            result = sync_all(tally_host, int(tally_port), db_path, progress_callback)

            progress_bar.progress(1.0)

            if result.get("success"):
                status_text.empty()
                detail_text.empty()
                progress_bar.empty()

                st.success(f"✅ Sync complete! Company: **{result.get('company', 'Unknown')}**")

                # Show summary
                stats = result.get("stats", {})
                if stats:
                    st.markdown("#### Data Summary")
                    scol1, scol2, scol3, scol4 = st.columns(4)
                    scol1.metric("Groups", stats.get("groups", 0))
                    scol2.metric("Ledgers", stats.get("ledgers", 0))
                    scol3.metric("Stock Items", stats.get("stock_items", 0))
                    scol4.metric("Vouchers", stats.get("vouchers", 0))

                    scol5, scol6, scol7, scol8 = st.columns(4)
                    scol5.metric("Accounting Entries", stats.get("accounting_entries", 0))
                    scol6.metric("Voucher Types", stats.get("voucher_types", 0))
                    scol7.metric("Cost Centres", stats.get("cost_centres", 0))
                    scol8.metric("Godowns", stats.get("godowns", 0))

                st.markdown("---")
                st.markdown("**🎉 Go to the main dashboard to start analyzing!** Use the sidebar to navigate.")

                time.sleep(1)
                st.rerun()
            else:
                status_text.empty()
                detail_text.empty()
                st.error(f"Sync failed: {result.get('error', 'Unknown error')}")

        except ImportError:
            st.error("tally_sync module not found. Please ensure tally_sync.py is in the app directory.")
        except Exception as e:
            st.error(f"Sync error: {e}")
            import traceback
            st.code(traceback.format_exc())

# ── INSTRUCTIONS ────────────────────────────────────────────────────────────

st.markdown("---")

with st.expander("📖 How to set up TallyPrime for connection", expanded=False):
    st.markdown("""
    ### Step 1: Enable XML Server in TallyPrime

    1. Open **TallyPrime**
    2. Press **F1** (Help) → **Settings** → **Connectivity**
    3. Set **"Enable ODBC Server"** to **Yes**
    4. The default port is **9000** — you can change it if needed
    5. Press **Ctrl+A** to save

    ### Step 2: Load Your Company

    Make sure your company data is loaded in TallyPrime. The company you see on the Gateway of Tally screen is the one that will be synced.

    ### Step 3: Connect from This App

    - If TallyPrime is on **this computer**: use `localhost` as the host
    - If TallyPrime is on **another computer on the same network**: use that computer's IP address
      - To find the IP: Open CMD on the Tally computer and run `ipconfig`
      - Look for the IPv4 Address (e.g., `192.168.1.16`)

    ### Step 4: Sync

    1. Click **"Test Connection"** to verify
    2. Click **"Sync All Data"** to extract everything
    3. Navigate to the **Dashboard** from the sidebar

    ### Notes
    - Sync takes 30-60 seconds depending on data volume
    - You can re-sync anytime to get the latest data
    - The data is stored locally — no internet required after sync
    """)

with st.expander("🔧 Advanced: Running on a Network", expanded=False):
    st.markdown("""
    ### Accessing from another device

    To access this dashboard from another device on the same network:

    ```bash
    streamlit run app.py --server.address 0.0.0.0 --server.port 8501
    ```

    Then open `http://<your-ip>:8501` from any browser on the network.

    ### Firewall

    Make sure port **9000** (Tally) and port **8501** (this app) are allowed through your firewall.
    """)

# ── FOOTER ──────────────────────────────────────────────────────────────────

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #888; font-size: 0.85rem;">
    <strong>Seven Labs Vision</strong> — AI-Powered Tally Analytics Platform<br>
    Built by CA Raghav Bansal | © 2025-2026
</div>
""", unsafe_allow_html=True)
