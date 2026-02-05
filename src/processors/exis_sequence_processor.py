import os
import json
import re
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from typing import Iterator, Dict, Any, Optional, Tuple
import logging
from dotenv import load_dotenv

import polars as pl
from Bio.Data.IUPACData import protein_letters

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

load_dotenv()

# Enviroment Variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def sanitize_column_names(ldf: pl.DataFrame) -> pl.DataFrame:
    """
    Sanitizes the column names in the current DataFrame to Eesure normalization.

    Args:
        ldf (pl.DataFrame): lazy DataFrame with possible unformatted column names. 

    Returns:
        pl.DataFrame: lazy DataFrame with normalized column names. 
    """
    def clean_col_name(col_name: str) -> str:
        """
        Applies lowercase, strip, and '_' transformations to the name to normalize it.
        
        If column starts with a number postgres objects to it. Move number until it reaches an underscore.

        Args:
            col_name (str): Unformatted name of the column in the DataFrame. 

        Returns:
            str: Normalized Column Name.
        """
        # Base normalization
        new_name = re.sub(r'[^a-z0-9]', '_', col_name.lower().strip())
        new_name = re.sub(r'_+', '_', new_name).strip('_')
        
        # Fix leading digits by moving them before the first underscore
        m = re.match(r'^(\d+)([a-z_].*)$', new_name)
        if m:
            leading_digits, rest = m.groups()
            if '_' in rest:
                first, remainder = rest.split('_', 1)
                new_name = f"{first}{leading_digits}_{remainder}"
            else:
                # No underscore: just move digits to the end
                new_name = f"{rest}{leading_digits}"

        return new_name
    
    try:
        # Run cleaning on each column name
        rename_map = {col: clean_col_name(col) for col in ldf.collect_schema().names()}
        new_ldf = ldf.rename(rename_map)
        
        logger.info("✅ Column Cleaning Complete")
        
        return new_ldf
    except Exception as e:
        # If we cannot clean we need to manually investigate csv or pipeline 
        # for why it failed before we can allow data entry into db. 
        logger.error("❌ Error caught on cleaning the csv column names with error: %s", e)
        raise
    
def compare_columns_with_current_table(ldf: pl.DataFrame):
    """
    Load columns from exis sequence postgres database table and check its columns against cleaned csv columns.

    Args:
        ldf (pl.DataFrame): Lazy DataFrame with possibly correct formatted columns.
    """
    try: 
        # Fetch columns from postgres
        query = "SELECT * FROM public.sbc_exis_sequences LIMIT 0"
        postgres_seq_df = pl.read_database_uri(query=query, uri=URI, engine='adbc')
        
        postgres_seq_df_set = set(postgres_seq_df.columns)
        ldf_set = set(ldf.collect_schema().names())
    except Exception as e:
        logger.error("❌ Error caught on loading the EXIS Sequences table: %s", e)
        raise
        
    if postgres_seq_df_set == ldf_set:
        logger.info("✅ Columns in CSV match with Columns in Database")
    else:
        missing_from_csv = postgres_seq_df_set - ldf_set
        missing_from_postgres = ldf_set - postgres_seq_df_set
        
        # discard() (safe for non-existent items)
        # source_key is rarely in upload files
        missing_from_csv.discard('source_key')
        
        if missing_from_csv:
            logger.error("❌ Mismatch detected in Exis Seq and CSV. Missing from CSV: %s, Missing from Postgres: %s", missing_from_csv, missing_from_postgres)
            # This is a fatal issue and the job must terminate as we cannot handle column name mismatch
            raise 
        else:
            # Not great but expected
            # Can move forward as the postgres sequence table column set exists within the set of columns in the csv.
            logger.debug("🔍 Mismatch detected in Exis Seq and CSV. Missing from CSV: %s, Missing from Postgres: %s", missing_from_csv, missing_from_postgres)
            
def sanitize_exis_runs(ldf: pl.DataFrame) -> pl.DataFrame:
    """
    Sanitize Exis Runs to ensure they are cast correctly as i64. 

    Args:
        ldf (pl.DataFrame): Lazy DataFrame with correct formatted columns. 

    Returns:
        pl.DataFrame: Lazy DataFrame with correct formatted exis runs. 
    """
    try:
        return ldf.with_columns(
            pl.col("exis_run")
              .str.replace_all(r"[A-Za-z]", "")
              .cast(pl.Int64, strict=False)
              .alias("exis_run")
        )
    except Exception as e:
        logger.error(
            "❌ Error sanitizing exis_run column: %s", e, exc_info=True
        )
        raise
        
def investigate_join_conflict(anti_ldf: pl.DataFrame, other_ldf: pl.DataFrame, anti_id: str, other_id: str, anti_columns_of_interest: list, other_columns_of_interest: list) -> pl.DataFrame:
    """
    Discover and log the source of conflict of each entry with an entry conflict. 

    Args:
        anti_ldf (pl.DataFrame): Lazy DataFrame with conflicting entries.
        other_ldf (pl.DataFrame): Other Lazy Dataframe to aid in investigation.
        anti_id (str): ID column name used in the anti Lazy Dataframe
        other_id (str): ID column name used in the other Lazy Dataframe
        anti_columns_of_interest (list): List of Columns in anti Lazy Dataframe needed for the investigation (Should not include ID but can handle if it is there).
        other_columns_of_interest (list): List of Columns in other Lazy Dataframe needed for the investigation (Should not include ID but can handle if it is there).
    Returns:
        pl.DataFrame: Lazy DataFrame audit table with identifiers and list of Columns containing problems so a specific id.
    """
    
    try:
        # Dictionary to track conflicts by ID
        conflicts_by_id = {}
        
        # check if error is on the id
        debug_ldf = (
            anti_ldf
            .join(
                other=other_ldf,
                how='anti',
                left_on=anti_id,
                right_on=other_id
            )
        )
        
        if debug_ldf.select(pl.len()).collect().item() != 0:
            # print(debug_ldf.collect())
            id_lst = (
                debug_ldf
                .select(anti_id)
                .collect()
                .get_column(anti_id)
                .to_list()
            )
            logger.debug('❌ CSV and Cross Reference cannot join on id entries: %s', id_lst)
            
            # Track ID conflicts
            for id_val in id_lst:
                conflicts_by_id[id_val] = [anti_id]

            # print(conflicts_by_id)
            
        # clean anti ldf of entries that cannot be checked further (if any)
        new_anti_ldf = (
            anti_ldf.join(
                other=debug_ldf,
                how='anti',
                on=anti_ldf.collect_schema().names()
            )
        )
        
        # Do deep dive into each column
        for i in range(len(anti_columns_of_interest)):
            if anti_columns_of_interest[i] == anti_id:
                continue
            
            debug_ldf = (
                new_anti_ldf
                .join(
                    other=other_ldf,
                    how='anti',
                    left_on=[anti_columns_of_interest[i], anti_id],
                    right_on=[other_columns_of_interest[i], other_id]
                )
            )
            
            if debug_ldf.select(pl.len()).collect().item() != 0:
                # print(debug_ldf.collect())
                conflict_col = anti_columns_of_interest[i]
                
                # Get IDs with this conflict
                id_lst = (
                    debug_ldf
                    .select(anti_id)
                    .collect()
                    .get_column(anti_id)
                    .to_list()
                )
                logger.debug('❌ CSV and Cross Reference cannot join on %s column for entries: %s', conflict_col, id_lst)
                
                # Track conflicts for each ID
                for id_val in id_lst:
                    if id_val in conflicts_by_id:
                        conflicts_by_id[id_val].append(conflict_col)
                    else:
                        conflicts_by_id[id_val] = [conflict_col]
        
        # print(conflicts_by_id)
        # Build the final error record DataFrame
        if conflicts_by_id:
            # Start with the original anti_ldf data
            error_rec_ldf = (
                anti_ldf
                .filter(pl.col(anti_id).is_in(list(conflicts_by_id.keys())))
                .with_columns(
                    pl.col(anti_id).map_elements(
                        lambda x: conflicts_by_id.get(x, []),
                        return_dtype=pl.List(pl.String)
                    ).alias("conflict_columns")
                )
            ).with_columns(
                pl.when(
                    pl.col('conflict_columns').list.contains(anti_id)
                )
                .then(pl.lit('no'))
                .otherwise(pl.lit('yes'))
                .alias('resolved')  # code will resolve the conflict unless it is prellis id
            )
            # print(error_rec_ldf.collect())
            return error_rec_ldf
        else:
            # No conflicts found, return empty with correct schema
            error_schema = {
                **anti_ldf.collect_schema(),
                'conflict_columns': pl.List(pl.String)
            }
            return pl.LazyFrame(schema=error_schema)
        
    except Exception as e:
        logger.error("❌ Error caught on investigating CSV and cross reference table conflicts with error: %s", e)
        raise
      
def compare_entries_with_cross_reference(ldf: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Load data from postgres cross reference postgres database table and check its entires against csv entries.

    Args:
        ldf (pl.DataFrame): Lazy DataFrame with correct formatted columns.

    Returns:
        pl.DataFrame: Lazy DataFrame with conflicting rows removed (if any). 
        pl.DataFrame: Lazy DataFrame audit table with identifiers and list of Columns containing problems so a specific id.
    """
    # Define Columns of interest 
    postgres_reference_ldf_columns_of_interest = ['sample_id', 'alias', 'exis_run', 'antigen']
    ldf_columns_of_interest = ['prellis_mabs_expressed', 'sequence_id', 'exis_run', 'antigen']
    
    try: 
        # Fetch cross reference table
        query = "SELECT * FROM public.sbc_antibody_cross_reference"
        postgres_reference_df = pl.read_database_uri(query=query, uri=URI, engine='adbc').select(postgres_reference_ldf_columns_of_interest)
        
        # create cross reference df
        postgres_reference_ldf = (
            postgres_reference_df
            .with_columns(
                pl.col('exis_run').str.replace_all(r"[A-Za-z]", "").cast(pl.Int64, strict=False),
            )
        ).lazy()
    except Exception as e:
        logger.error("❌ Error caught on loading and preparing the cross reference table for analysis with error: %s", e)
        raise
    
    # postgres reference df no longer needed now that we have the lazy df version
    del postgres_reference_df
        
    try:
        # Anti join to see which entries in the CSV do not match with the entries in cross reference table
        anti_join_ldf = (
            ldf
            .select(ldf_columns_of_interest)
            .join(
                postgres_reference_ldf,
                how="anti",
                left_on=ldf_columns_of_interest,
                right_on=postgres_reference_ldf_columns_of_interest,
            )
        )
    except Exception as e:
        logger.error("❌ Error caught while trying to compare the CSV with the cross reference table with error: %s", e)
        raise
        
    if anti_join_ldf.select(pl.len()).collect().item() != 0:
        # Join Error Detected!
        logger.debug("🔍 Join Error Detected. Investigator Triggered.")
        
        anti_id = 'prellis_mabs_expressed'
        cross_reference_id = 'sample_id'
        
        error_rec_ldf = investigate_join_conflict(anti_join_ldf, postgres_reference_ldf, anti_id, cross_reference_id, ldf_columns_of_interest, postgres_reference_ldf_columns_of_interest)
        
        # regardless of results of the investigation, assume the cross reference to be 
        # the authority and to replace conflicting entries of interest with cross reference entries of interest
        
        # First, check if there are any IDs in the CSV that donot exist in cross reference at all
        try:
            invalid_ids_ldf = (
                ldf
                .select(anti_id)
                .unique()
                .join(
                    postgres_reference_ldf.select(cross_reference_id).unique(),
                    left_on=anti_id,
                    right_on=cross_reference_id,
                    how='anti'
                )
            )
            
            invalid_count = invalid_ids_ldf.select(pl.len()).collect().item()
            
            if invalid_count > 0:
                invalid_id_list = (
                    invalid_ids_ldf
                    .collect()
                    .get_column(anti_id)
                    .to_list()
                )
                logger.warning("⚠️ Found %s IDs in CSV that don't exist in cross reference table. Dropping: %s", 
                            invalid_count, invalid_id_list)
                
                # Remove rows with invalid IDs
                ldf = (
                    ldf.join(
                        other=invalid_ids_ldf,
                        how='anti',
                        on=anti_id
                    )
                )
                
                # Remove from anti ldf
                anti_join_ldf = (
                    anti_join_ldf.join(
                        other=invalid_ids_ldf,
                        how='anti',
                        on=anti_id
                    )
                )
                
                # print(ldf.collect())
                # print(anti_join_ldf.collect())
        except Exception as e:
            logger.error("❌ Error caught while checking for invalid IDs with error: %s", e)
            raise
        
        # Replace conflicting entries with values from cross reference table
        # First, get the IDs that have conflicts
        conflicting_ids = anti_join_ldf.select(anti_id)
        
        # Join with cross reference to get the correct values for these IDs
        corrected_data = (
            conflicting_ids
            .join(
                postgres_reference_ldf,
                left_on=anti_id,
                right_on=cross_reference_id,
                how='left'
            )
            .rename({
                'alias': 'sequence_id'
            })
        )
        
        # print(corrected_data.collect())
        
        # Remove conflicting rows from original ldf
        ldf_without_conflicts = (
            ldf.join(
                other=conflicting_ids,
                how='anti',
                on=anti_id
            )
        )
        
        # Get all columns from ldf that are NOT in the columns we're updating
        non_updated_cols = [col for col in ldf.collect_schema().names() if col not in ldf_columns_of_interest]
        
        # Join corrected data with the non-updated columns from original ldf
        corrected_rows = (
            corrected_data
            .join(
                ldf.select([anti_id] + non_updated_cols),
                on=anti_id,
                how='left'
            )
        )
        
        # Concatenate the non-conflicting rows with the corrected rows
        ldf = pl.concat([ldf_without_conflicts, corrected_rows], how='diagonal')
        
        # Sanity Check. Double Check there are now no more conflicts
        try:
            anti_join_ldf = (
                ldf
                .select(ldf_columns_of_interest)
                .join(
                    postgres_reference_ldf,
                    how="anti",
                    left_on=ldf_columns_of_interest,
                    right_on=postgres_reference_ldf_columns_of_interest,
                )
            )
        except Exception as e:
            logger.error("❌ Error caught while trying to compare the CSV with the cross reference table with error: %s", e)
            raise
            
        if anti_join_ldf.select(pl.len()).collect().item() == 0:
        
            logger.info("✅ Replaced %d conflicting rows with cross reference data", 
                    anti_join_ldf.select(pl.len()).collect().item())
            
        else:
            raise
    else:
        # No conflicts found - create empty error record DataFrame
        error_schema = {
            **ldf.collect_schema(),
            'conflict_columns': pl.List(pl.String),
            'resolved': pl.String
        }
        error_rec_ldf = pl.LazyFrame(schema=error_schema)
        
        
    return ldf, error_rec_ldf

def clean_seq(ldf: pl.DataFrame, is_aa: bool) -> pl.DataFrame:
    """
    Go through each column with Heavy/Light Chain Sequence Data and clean the sequences to stripped, uppercase entries.

    Args:
        ldf (pl.DataFrame): Lazy DataFrame with correct formatted columns and confirmed valid id.
        is_aa (bool): True if you want to apply analysis to Amino Acid Heavy/Light Chain Sequence Data.
                      False if  you want to apply analysis to Nucleotide Heavy/Light Chain Sequence Data.

    Returns:
        pl.DataFrame: Lazy DataFrame with cleaned sequence entries
    """
    if is_aa:
        sequence_cols = [
            'fwr1_aa_heavy',
            'cdr1_aa_heavy',
            'fwr2_aa_heavy',
            'cdr2_aa_heavy',
            'fwr3_aa_heavy',
            'cdr3_aa_heavy',
            'fwr4_aa_heavy',
            'trimmed_aa_heavy',
            'fwr1_aa_light',
            'cdr1_aa_light',
            'fwr2_aa_light',
            'cdr2_aa_light',
            'fwr3_aa_light',
            'cdr3_aa_light',
            'fwr4_aa_light',
            'trimmed_aa_light',
        ]
    else:
        sequence_cols = [
            'fwr1_nu_heavy',
            'cdr1_nu_heavy',
            'fwr2_nu_heavy',
            'cdr2_nu_heavy',
            'fwr3_nu_heavy',
            'cdr3_nu_heavy',
            'fwr4_nu_heavy',
            'trimmed_nu_heavy',
            'fwr1_nu_light',
            'cdr1_nu_light',
            'fwr2_nu_light',
            'cdr2_nu_light',
            'fwr3_nu_light',
            'cdr3_nu_light',
            'fwr4_nu_light',
            'trimmed_nu_light',
        ]
    
    exprs = [
        pl.col(seq_col)
        .str.strip_chars()
        .str.to_uppercase()
        .alias(seq_col)
        for seq_col in sequence_cols
    ]

    return ldf.with_columns(exprs)

def validate_seq(ldf: pl.DataFrame, error_rec_ldf: pl.DataFrame, allow_ambiguous: bool, is_aa: bool, allow_null: bool = False) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Go through each column with Heavy/Light Chain Sequence Data and remove any rows with invalid Sequences.
    Args:
        ldf (pl.DataFrame): Lazy DataFrame with correct formatted columns, confirmed valid id, cleaned sequence entries.
        error_rec_ldf (pl.DataFrame): Lazy DataFrame with records or previous sources of conflicts.
        allow_ambiguous (bool): True if constraints on protein letters are too strict and valid 
                                protein letters should include letters which indicate ambiguity in a sequence. 
                                False Otherwise. 
        is_aa (bool): True if you want to apply analysis to Amino Acid Heavy/Light Chain Sequence Data.
                      False if  you want to apply analysis to Nucleotide Heavy/Light Chain Sequence Data.
        allow_null (bool): True if null values in sequence columns should be considered valid.
                          False if null values should be treated as invalid. Default is False.
    Returns:
        pl.DataFrame: Lazy DataFrame with conflicting rows removed (if any). 
        pl.DataFrame: Lazy DataFrame with sequence records of sources of conflicts appended to previous records. 
    """
    if is_aa:
        # Amino Acid chains use protein letters and possibly some ambiguous letters in their sequences
        canonical = set(protein_letters)
        extended = canonical | set("BXZJUO")
        sequence_cols = [
            'fwr1_aa_heavy',
            'cdr1_aa_heavy',
            'fwr2_aa_heavy',
            'cdr2_aa_heavy',
            'fwr3_aa_heavy',
            'cdr3_aa_heavy',
            'fwr4_aa_heavy',
            'trimmed_aa_heavy',
            'fwr1_aa_light',
            'cdr1_aa_light',
            'fwr2_aa_light',
            'cdr2_aa_light',
            'fwr3_aa_light',
            'cdr3_aa_light',
            'fwr4_aa_light',
            'trimmed_aa_light',
        ]
    else:
        # Nucleotide chains use dna letters and possibly some ambiguous letters in their sequences
        canonical = set("ACGT")
        extended = canonical | set("RYWSKMBDHVN")
        sequence_cols = [
            'fwr1_nu_heavy',
            'cdr1_nu_heavy',
            'fwr2_nu_heavy',
            'cdr2_nu_heavy',
            'fwr3_nu_heavy',
            'cdr3_nu_heavy',
            'fwr4_nu_heavy',
            'trimmed_nu_heavy',
            'fwr1_nu_light',
            'cdr1_nu_light',
            'fwr2_nu_light',
            'cdr2_nu_light',
            'fwr3_nu_light',
            'cdr3_nu_light',
            'fwr4_nu_light',
            'trimmed_nu_light',
        ]

    def invalid_expr(col: str) -> pl.Expr:
        """
        Find and return rows with invalid sequence expressions in the given column.
        Args:
            col (str): column name for the sequences.
        Returns:
            pl.Expr: symbolic expression for finding invalid sequences. 
        """
        allowed = extended if allow_ambiguous else canonical
        pattern = f"^[{''.join(sorted(allowed))}]+$"
        c = pl.col(col)
        
        if allow_null:
            # Only check non-null values for validity
            return (
                c.is_not_null()
                & ~c
                    .str.strip_chars()
                    .str.to_uppercase()
                    .str.contains(pattern)
            )
        else:
            # Treat null as invalid OR check pattern
            return (
                c.is_null()
                | (
                    c.is_not_null()
                    & ~c
                        .str.strip_chars()
                        .str.to_uppercase()
                        .str.contains(pattern)
                )
            )

    ldf_columns_of_interest = ['prellis_mabs_expressed', 'sequence_id', 'exis_run', 'antigen']
    
    # Create list of invalid column names, filtering out nulls
    invalid_cols_expr = (
        pl.concat_list([
            pl.when(invalid_expr(seq))
            .then(pl.lit(seq))
            .otherwise(pl.lit(None, dtype=pl.String)) 
            for seq in sequence_cols
        ])
        .list.drop_nulls()  # Remove null entries from the list
        .list.len()
        .pipe(lambda x: pl.when(x > 0).then(
            pl.concat_list([
                pl.when(invalid_expr(seq))
                .then(pl.lit(seq))
                .otherwise(pl.lit(None, dtype=pl.String)) 
                for seq in sequence_cols
            ]).list.drop_nulls()
        ).otherwise(None))
        .alias("conflict_columns")
    )
    
    # Create an audit of rows with problem sequences and what the col names are for 
    # the problem sequences as it relates to the prellis id. 
    audit_ldf = (
        ldf
        .with_columns(invalid_cols_expr)
        .filter(pl.col("conflict_columns").is_not_null())
        .select(ldf_columns_of_interest + ["conflict_columns"])
    ).with_columns(
        pl.lit('no')
        .alias("resolved")
    )
    
    # print(audit_ldf.collect())
    
    # Concat previous records with current records
    error_rec_ldf = (
        pl.concat(
            [error_rec_ldf, audit_ldf],
            how='vertical'
        )
    )
    
    # Clear ldf of rows with problem sequences
    ldf = (
        ldf.join(
            other=audit_ldf,
            how='anti',
            on='prellis_mabs_expressed'
        )
    )
    
    return ldf, error_rec_ldf

def process_exis_sequence_uploads(bucket: str, key: str) -> None:
    """
    Process the CSV assuming Exis Sequence Table Schema Uploads. 
    
    Processing includes:
        - Sanitize Column Names
        - ID, Antigen, Alias, and Exis Run Cross Reference
        - Nucleotide and Amino Acid Validation
        - type checking and casting

    Args:
        bucket (str): Name of the AWS S3 Bucket. 
        key (str): File Path of CSV located in the specificed bucket.
    """
    logger.info("🔍 Beginning Processing of bucket=%s key=%s for EXIS Sequences Table", bucket, key)
    # Step 1: Load CSV directly into a DataFrame
    ldf = pl.scan_csv(f"s3://{bucket}/{key}")
    
    # Step 1.5: Drop any rows that are completely null
    ldf = ldf.filter(
        ~pl.all_horizontal(pl.all().is_null())
    )
    
    logger.info("🔍 Sanitizing Column Names")
    # Step 2: Sanitize Column Names
    ldf = sanitize_column_names(ldf)
    
    logger.info("🔍 Evaluating Cleaning")
    # Step 3: Evaluate results of cleaning
    compare_columns_with_current_table(ldf)
    
    # Step 4: Compare entries of CSV with entries in Master Tracker
    logger.info("🔍 Cross Referencing")
    ldf = compare_entries_with_cross_reference(ldf)
    
    # Step 5: Sanitize Amino Acid (aa) Heavy and Light Chain Sequences
    ldf = clean_seq(ldf=ldf, is_aa=True)
    
    # Step 6: Validate Amino Acid (aa) Heavy and Light Chain Sequences
    ldf = validate_seq(ldf=ldf, allow_ambiguous=True, is_aa=True)
    
    # Step 7: Sanitize Nucleotide (nu) Heavy and Light Chain Sequences
    ldf = clean_seq(ldf=ldf, is_aa=False)
    
    # Step 8: Validate Nucleotide (nu) Heavy and Light Chain Sequences
    ldf = validate_seq(ldf=ldf, allow_ambiguous=True, is_aa=False)

def main(): 
    # Step 1: Load CSV directly into a DataFrame
    ldf = pl.scan_csv(f"/Users/alexedwards/Documents/Prellis/lambdas/NGS21_iRep_FINAL_Sept_16.csv")
    
    logger.info("🔍 Sanitizing Column Names")
    # Step 2: Sanitize Column Names
    ldf = sanitize_column_names(ldf)
    
    logger.info("🔍 Evaluating Cleaning")
    # Step 3: Evaluate results of cleaning
    compare_columns_with_current_table(ldf)
    
    # Step 4: Sanitize Exis Runs
    ldf =  sanitize_exis_runs(ldf)
    
    # Step 5: Compare entries of CSV with entries in Master Tracker
    logger.info("🔍 Cross Referencing")
    ldf, error_rec_ldf = compare_entries_with_cross_reference(ldf)
    
    # Step 6: Sanitize Amino Acid (aa) Heavy and Light Chain Sequences
    logger.info("🔍 Cleaning Amino Acids")
    ldf = clean_seq(ldf=ldf, is_aa=True)
    
    # Step 7: Validate Amino Acid (aa) Heavy and Light Chain Sequences
    logger.info("🔍 Validating Amino Acids")
    ldf, error_rec_ldf = validate_seq(ldf, error_rec_ldf, allow_ambiguous=True, is_aa=True, allow_null=False)
    
    # Step 8: Sanitize Nucleotide (nu) Heavy and Light Chain Sequences
    logger.info("🔍 Cleaning Nucleotides")
    ldf = clean_seq(ldf=ldf, is_aa=False)
    
    # Step 9: Validate Nucleotide (nu) Heavy and Light Chain Sequences
    logger.info("🔍 Validating Nucleotides")
    ldf, error_rec_ldf = validate_seq(ldf, error_rec_ldf, allow_ambiguous=True, is_aa=False, allow_null=True)
    
    df = ldf.collect()
    df.write_csv(f"/Users/alexedwards/Documents/Prellis/lambdas/output_results.csv")

    
if __name__ == "__main__":
    main()