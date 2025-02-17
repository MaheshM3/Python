import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO

# Database connection details
DB_HOST = "your_db_host"
DB_NAME = "your_db_name"
DB_USER = "your_db_user"
DB_PASS = "your_db_password"
DB_PORT = "your_db_port"

# Create SQLAlchemy engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

st.title("🔍 PostgreSQL Query Profiler")

# User inputs
date_selected = st.date_input("📅 Select Date")
location_selected = st.text_input("📍 Enter Location")

if st.button("🔄 Run Query"):
    with engine.connect() as conn:
        # Define queries
        query1 = f"""
        SELECT "Date", "ID", "Location", "Total Count", "DQ Count"
        FROM table1
        WHERE "Date" = '{date_selected}' AND "Location" = '{location_selected}'
        """
        
        query2 = f"""
        SELECT "Date", "ID", "Location", "Total Count", "DQ Count"
        FROM table2
        WHERE "Date" = '{date_selected}' AND "Location" = '{location_selected}'
        """

        # Fetch data
        df1 = pd.read_sql(query1, conn)
        df2 = pd.read_sql(query2, conn)

    # Display row counts
    st.subheader("📊 Dataset Row Counts")
    st.write(f"**Query 1 Row Count:** {len(df1)}")
    st.write(f"**Query 2 Row Count:** {len(df2)}")

    if df1.empty and df2.empty:
        st.warning("⚠ No data found for the selected Date and Location!")
    else:
        # Merge data for comparison
        merged_df = df1.merge(df2, on=["Date", "Location", "ID"], suffixes=("_Q1", "_Q2"), how="outer", indicator=True)

        # Handle missing values
        merged_df.fillna({"Total Count_Q1": 0, "Total Count_Q2": 0, "DQ Count_Q1": 0, "DQ Count_Q2": 0}, inplace=True)

        # Add "Match Status" column
        def get_match_status(row):
            if row["_merge"] == "left_only":
                return "Only in Query 1"
            elif row["_merge"] == "right_only":
                return "Only in Query 2"
            elif row["Total Count_Q1"] != row["Total Count_Q2"] or row["DQ Count_Q1"] != row["DQ Count_Q2"]:
                return "Mismatch"
            else:
                return "Matching"

        merged_df["Match Status"] = merged_df.apply(get_match_status, axis=1)

        # Filter dropdown
        match_filter = st.selectbox(
            "🔍 Filter by Match Status",
            options=["All", "Matching", "Mismatch", "Only in Query 1", "Only in Query 2"],
            index=0
        )

        # Apply filter
        if match_filter != "All":
            merged_df = merged_df[merged_df["Match Status"] == match_filter]

        # Interactive table
        st.subheader("📌 Comparison Results")
        st.data_editor(merged_df, use_container_width=True, hide_index=True)

        # Convert DataFrame to CSV for download
        csv_buffer = BytesIO()
        merged_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Download button
        st.download_button(
            label="📥 Download CSV",
            data=csv_buffer,
            file_name=f"comparison_results_{date_selected}.csv",
            mime="text/csv"
        )