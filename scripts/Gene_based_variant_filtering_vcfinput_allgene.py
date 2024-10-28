import argparse
from argparse import RawTextHelpFormatter
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, DoubleType
import glow
import sys

parser = argparse.ArgumentParser(
    description='Script of gene based variant filtering. \n\
    MUST BE RUN WITH spark-submit. For example: \n\
    spark-submit --driver-memory 10G Gene_based_variant_filtering.py',
    formatter_class=RawTextHelpFormatter)

parser = argparse.ArgumentParser()
parser.add_argument('--hgmd_var',
        help='HGMD variant parquet file dir')
parser.add_argument('--dbnsfp',
        help='dbnsfp annovar parquet file dir')
parser.add_argument('--clinvar', action='store_true', help='Include ClinVar data')
parser.add_argument('--consequences', action='store_true', help='Include consequences data')
parser.add_argument('--vcf_input',
        help='vcf input file')
parser.add_argument('--maf', default=0.0001,
        help='gnomAD and TOPMed max allele frequency')
parser.add_argument('--dpc_l', default=0.5,
        help='damage predict count lower threshold')
parser.add_argument('--dpc_u', default=1,
        help='damage predict count upper threshold')
parser.add_argument('--known_variants_l', nargs='+', default=['ClinVar', 'HGMD'],
                    help='known variant databases used, default is ClinVar and HGMD')
parser.add_argument('--aaf', default=0.2,
        help='alternative allele fraction threshold')
parser.add_argument('--output_basename', default='gene-based-variant-filtering',
        help='Recommand use the task ID in the url above as output file prefix. \
        For example 598b5c92-cb1d-49b2-8030-e1aa3e9b9fde is the task ID from \
    https://cavatica.sbgenomics.com/u/yiran/variant-workbench-testing/tasks/598b5c92-cb1d-49b2-8030-e1aa3e9b9fde/#set-input-data')
parser.add_argument('--study_id')
parser.add_argument('--spark_executor_mem', type=int, default=4, help='Spark executor memory in GB')
parser.add_argument('--spark_executor_instance', type=int, default=1, help='Number of Spark executor instances')
parser.add_argument('--spark_executor_core', type=int, default=1, help='Number of Spark executor cores')
parser.add_argument('--spark_driver_maxResultSize', type=int, default=1, help='Spark driver max result size in GB')
parser.add_argument('--sql_broadcastTimeout', type=int, default=300, help='Spark SQL broadcast timeout in seconds')
parser.add_argument('--spark_driver_core', type=int, default=1, help='Number of Spark driver cores')
parser.add_argument('--spark_driver_mem', type=int, default=4, help='Spark driver memory in GB')

args = parser.parse_args()

# Create SparkSession
spark = SparkSession \
    .builder \
    .appName('glow_pyspark') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,io.projectglow:glow-spark3_2.12:2.0.0') \
    .config('spark.jars.excludes', 'org.apache.hadoop:hadoop-client,io.netty:netty-all,io.netty:netty-handler,io.netty:netty-transport-native-epoll') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.sql.debug.maxToStringFields', '0') \
    .config('spark.hadoop.io.compression.codecs', 'io.projectglow.sql.util.BGZFCodec') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider') \
    .config('spark.kryoserializer.buffer.max', '512m') \
    .config('spark.executor.memory', f'{args.spark_executor_mem}G') \
    .config('spark.executor.instances', args.spark_executor_instance) \
    .config('spark.executor.cores', args.spark_executor_core) \
    .config('spark.driver.maxResultSize', f'{args.spark_driver_maxResultSize}G') \
    .config('spark.sql.broadcastTimeout', args.sql_broadcastTimeout) \
    .config('spark.driver.cores', args.spark_driver_core) \
    .config('spark.driver.memory', f'{args.spark_driver_mem}G') \
    .getOrCreate()
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# parameter configuration
gnomAD_TOPMed_maf = args.maf
dpc_l = args.dpc_l
dpc_u = args.dpc_u
known_variants_l = args.known_variants_l
aaf = float(args.aaf)
output_basename = args.output_basename
vcf_input_path = args.vcf_input
biospecimen_id_value = None  # Declare as global

# customized tables loading
hg38_HGMD_variant = spark.read.parquet(args.hgmd_var)
dbnsfp_annovar = spark.read.parquet(args.dbnsfp)
if args.clinvar:
    clinvar = spark.read.format("delta") \
        .load("s3a://kf-strides-public-vwb-prd/clinvar")
if args.consequences:
    consequences = spark.read.format("delta") \
        .load('s3a://kf-strides-registered-vwb-prd/enriched/consequences')
    
# Standardize the 'gnomad_3_1_1_AF' column
def standardize_gnomad_column(df):
    if 'INFO_gnomad_3_1_1_AF' in df.columns:
        # Check the current type of the column
        column_type = df.schema['INFO_gnomad_3_1_1_AF'].dataType
        if isinstance(column_type, ArrayType):
            # If it's an array, take the first element or handle it as needed
            df = df.withColumn('INFO_gnomad_3_1_1_AF', F.col('INFO_gnomad_3_1_1_AF').getItem(0).cast(DoubleType()))
        elif isinstance(column_type, DoubleType):
            # If it's already a double, no change needed
            pass
    return df

# Read the VCF file and process the data
vcf_df = spark \
    .read.format('vcf') \
    .load(vcf_input_path) \
    .withColumn('chromosome', 
                F.regexp_replace(F.col('contigName'), r'(?i)^chr[^\dXxYyMm]*', '')) \
    .withColumn('alternate', 
                F.when(F.size(F.col('alternateAlleles')) == 1, 
                        F.col('alternateAlleles').getItem(0)).otherwise(F.lit(None))) \
    .withColumnRenamed('referenceAllele', 'reference') \
    .drop('contigName', 'referenceAllele', 'alternateAlleles')
# Update the start position
updated_vcf_df = vcf_df.withColumn("start", F.col("start") + 1)
standardized_vcf_df = standardize_gnomad_column(updated_vcf_df)


# gene based variant filtering
def gene_based_filt(gnomAD_TOPMed_maf, dpc_l, dpc_u,
                known_variants_l, aaf, hg38_HGMD_variant, dbnsfp_annovar, clinvar, 
                consequences, vcf_input):
    #  Actual running step, generating table t_output
    cond = ['chromosome', 'start', 'reference', 'alternate']

    # Table consequences, restricted to canonical annotation and input genes/study IDs
    c_csq = ['consequence', 'vep_impact', 'symbol', 'ensembl_gene_id', 'refseq_mrna_id', 'hgvsc',
            'hgvsp']
    t_csq = consequences.where((F.col('original_canonical') == 'true')) \
        .select(cond + c_csq)
    chr_list = [c['chromosome'] for c in t_csq.select('chromosome').distinct().collect()]

    # Table dbnsfp_annovar, added a column for ratio of damage predictions to all predictions
    c_dbn = ['DamagePredCount', 'PredCountRatio_D2T', 'TWINSUK_AF', 'ALSPAC_AF', 'UK10K_AF']
    t_dbn = dbnsfp_annovar \
        .where(F.col('chromosome').isin(chr_list)) \
        .withColumn('PredCountRatio_D2T',
                    F.when(F.split(F.col('DamagePredCount'), '_')[1] == 0, F.lit(None).cast(DoubleType())) \
                    .otherwise(
                        F.split(F.col('DamagePredCount'), '_')[0] / F.split(F.col('DamagePredCount'), '_')[1])) \
        .select(cond + c_dbn)


    # Table ClinVar, restricted to those seen in variants and labeled as pathogenic/likely_pathogenic
    c_clv = ['VariationID', 'clin_sig', 'conditions']
    t_clv = clinvar \
        .withColumnRenamed('name', 'VariationID') \
        .where(F.array_contains(F.col('clin_sig'), 'Pathogenic') \
                | F.array_contains(F.col('clin_sig'), 'Likely_pathogenic')) \
        .select(cond + c_clv)

    # Table HGMD, restricted to those seen in variants and labeled as DM or DM?
    c_hgmd = ['HGMDID', 'variant_class', 'phen']
    t_hgmd = hg38_HGMD_variant \
        .withColumnRenamed('id', 'HGMDID') \
        .where(F.col('variant_class').startswith('DM')) \
        .select(cond + c_hgmd)


    # Table vcf_input, restricted to input genes, chromosomes of those genes, input study IDs, and vcf_input where alternate allele
    # is present, plus adjusted calls based on alternative allele fraction in the total sequencing depth
    c_ocr = ['ad', 'dp', 'variant_allele_fraction', 'calls', 'adjusted_calls', 'filters', 'biospecimen_id', 'INFO_gnomad_3_1_1_AF', 'max_gnomad_topmed']
    t_ocr = vcf_input.withColumn('variant_allele_fraction', 
                                F.col('genotypes')['alleleDepths'][0][1] / 
                                (F.col('genotypes')['alleleDepths'][0][0] + F.col('genotypes')['alleleDepths'][0][1])) \
                    .withColumn('adjusted_calls', 
                                F.when(F.col('variant_allele_fraction') < aaf, F.array(F.lit(0), F.lit(0))) \
                                .when((F.col('variant_allele_fraction') >= aaf) & 
                                    (F.col('variant_allele_fraction') <= (1 - aaf)), 
                                    F.array(F.lit(0), F.lit(1))) \
                                .when(F.col('variant_allele_fraction') > (1 - aaf), 
                                    F.array(F.lit(1), F.lit(1))) \
                                .otherwise(F.col('genotypes')['calls'][0])) \
                    .withColumn('ad',F.col('genotypes')['alleleDepths'].getItem(0)) \
                    .withColumn('dp',F.col('genotypes')['depth'].getItem(0)) \
                    .withColumn('calls',F.col('genotypes')['calls'].getItem(0)) \
                    .withColumn('biospecimen_id',F.col('genotypes')['sampleId'].getItem(0)) \
                    .withColumn('max_gnomad_topmed', 
                                F.greatest(F.col('INFO_gnomad_3_1_1_AF'), F.lit(0))) \
                    .where(F.col('chromosome').isin(chr_list) & 
                        (F.col('splitFromMultiAllelic') == 'false') & 
                        (F.col('adjusted_calls') != F.array(F.lit(0), F.lit(0)))) \
                    .select(cond + c_ocr)
    
    # Extract unique biospecimen_id
    unique_biospecimen_id = t_ocr.select('biospecimen_id').distinct().first()
    biospecimen_id_value = unique_biospecimen_id.biospecimen_id if unique_biospecimen_id else 'unknown'


    # Join consequences, and dbnsfp, restricted to those with MAF less than a threshold and PredCountRatio_D2T within a range
    t_occ_csq = t_csq \
        .join(t_ocr, cond)

    t_occ_csq_dbn = t_occ_csq \
        .join(t_dbn, cond, how='left') \
        .withColumn('flag', F.when( \
        (F.col('max_gnomad_topmed') < gnomAD_TOPMed_maf) \
        & (F.col('PredCountRatio_D2T').isNull() \
            | (F.col('PredCountRatio_D2T').isNotNull() \
            & (F.col('PredCountRatio_D2T') >= dpc_l) \
            & (F.col('PredCountRatio_D2T') <= dpc_u)) \
            ), 1) \
                    .otherwise(0))

    # Include ClinVar if specified
    if 'ClinVar' in known_variants_l :
        t_occ_csq_dbn = t_occ_csq_dbn \
            .join(t_clv, cond, how='left') \
            .withColumn('flag', F.when(F.col('VariationID').isNotNull(), 1).otherwise(t_occ_csq_dbn.flag))

    # Include HGMD if specified
    if 'HGMD' in known_variants_l :
        t_occ_csq_dbn = t_occ_csq_dbn \
            .join(t_hgmd, cond, how='left') \
            .withColumn('flag', F.when(F.col('HGMDID').isNotNull(), 1).otherwise(t_occ_csq_dbn.flag))


    # Finally join all together
    # t_output = F.broadcast(t_occ_csq_dbn) \
    t_output = t_occ_csq_dbn \
        .where(t_occ_csq_dbn.flag == 1) \
        .drop('flag')
    
    return (t_output)


# define output name and write table t_output
def write_output(t_output, output_basename):
    Date = list(spark.sql("select current_date()") \
                .withColumn("current_date()", F.col("current_date()").cast("string")) \
                .toPandas()['current_date()'])
    output_filename = f"{biospecimen_id_value}_" + "_".join(Date) + "_" + output_basename + ".tsv.gz"
    # Sort the output
    t_output_sorted = t_output.sort(
        F.asc(
            F.when(F.col('chromosome').isin(['X', 'Y', 'x', 'y']),
                    F.lpad(F.col('chromosome'), 2, '2'))
            .otherwise(F.lpad(F.col('chromosome'), 2, '0'))
        ),
        F.asc(F.col('start')))
    t_output_sorted.toPandas().to_csv(output_filename, sep="\t", index=False, na_rep='-', compression='gzip')

if args.hgmd_var is None:
    print("Missing hgmd_var parquet file", file=sys.stderr)
    exit(1)
if args.dbnsfp is None:
    print("Missing dbnsfp parquet file", file=sys.stderr)
    exit(1)
if args.clinvar is None:
    print("Missing clinvar parquet file", file=sys.stderr)
    exit(1)
if args.consequences is None:
    print("Missing consequences parquet file", file=sys.stderr)
    exit(1)

t_output = gene_based_filt(gnomAD_TOPMed_maf, dpc_l, dpc_u,
                    known_variants_l, aaf, hg38_HGMD_variant, dbnsfp_annovar, clinvar, 
                    consequences, standardized_vcf_df)
write_output(t_output, output_basename)