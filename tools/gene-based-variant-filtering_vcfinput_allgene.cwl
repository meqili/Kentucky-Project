cwlVersion: v1.2
class: CommandLineTool
label: Gene_Based_Variant_Filtering (VCF Input)
doc: |-
  Get a list of deleterious variants in interested genes from specified study cohort(s) in the Kids First program.
$namespaces:
  sbg: https://sevenbridges.com

requirements:
- class: ShellCommandRequirement
- class: ResourceRequirement
  coresMin: 16
  ramMin: $(inputs.spark_driver_mem * 1000)
- class: DockerRequirement
  dockerPull: pgc-images.sbgenomics.com/qqlii44/pyspark:3.5.1
- class: InitialWorkDirRequirement
  listing:
  - entryname: Gene_based_variant_filtering_vcfinput_allgene.py
    entry:
      $include: ../scripts/Gene_based_variant_filtering_vcfinput_allgene.py
- class: InlineJavascriptRequirement

inputs:
- id: spark_driver_mem
  doc: GB of RAM to allocate to this task
  type: int?
  default: 48
  inputBinding:
    position: 3
    prefix: --spark_driver_mem
- id: spark_executor_instance
  doc: number of instances used 
  type: int?
  default: 3
  inputBinding:
    position: 3
    prefix: --spark_executor_instance
- id: spark_executor_mem
  doc: GB of executor memory
  type: int?
  default: 34
  inputBinding:
    position: 3
    prefix: --spark_executor_mem
- id: spark_executor_core
  doc: number of executor cores
  type: int?
  default: 5
  inputBinding:
    position: 3
    prefix: --spark_executor_core
- id: spark_driver_core
  doc: number of driver cores
  type: int?
  default: 2
  inputBinding:
    position: 3
    prefix: --spark_driver_core
- id: spark_driver_maxResultSize
  doc: GB of driver maxResultSize
  type: int?
  default: 2
  inputBinding:
    position: 3
    prefix: --spark_driver_maxResultSize
- id: sql_broadcastTimeout
  doc: .config("spark.sql.broadcastTimeout", 36000)
  type: int?
  default: 36000
  inputBinding:
    position: 3
    prefix: --sql_broadcastTimeout
- id: hgmd_var
  doc: the latest HGMD variant  parquet file dir
  type: File
  sbg:suggestedValue:
    name: hg38_HGMD2024Q2_variant.tar.gz
    class: File
    path: 66a3b948eead7f7aca80d33b
- id: dbnsfp_annovar
  doc: dbnsfp annovar parquet file dir
  type: File
  sbg:suggestedValue:
    name: dbnsfp.tar.gz
    class: File
    path: 65b03e76b2d0f428e1c6f049
- id: clinvar
  type: boolean
  inputBinding:
    position: 3
    prefix: --clinvar
- id: consequences
  type: boolean
  inputBinding:
    position: 3
    prefix: --consequences
- id: gnomAD_TOPMed_maf
  doc: the max global AF across all gnomAD and TOPMed databases
  type: double?
  default: 0.0001
  inputBinding:
    prefix: --maf
    position: 3
    shellQuote: false
- id: alternative_allele_fraction
  doc: alternative allele fraction
  type: double?
  default: 0.2
  inputBinding:
    prefix: --aaf
    position: 3
    shellQuote: false
- id: damage_predict_count_lower
  type: double?
  default: 0.5
  inputBinding:
    prefix: --dpc_l
    position: 3
    shellQuote: false
- id: damage_predict_count_upper
  type: double?
  default: 1
  inputBinding:
    prefix: --dpc_u
    position: 3
    shellQuote: false
- id: known_variants_l
  doc: Check known variants in following database(s)
  type:
  - 'null'
  - name: buildver
    type: enum
    symbols:
    - Clinvar
    - HGMD
    - Clinvar HGMD
  default: Clinvar HGMD
  inputBinding:
    prefix: --known_variants_l
    position: 3
    shellQuote: false
- id: vcf_input
  doc: the path of vcf input file
  type: File
  inputBinding:
    prefix: --vcf_input
    position: 3
    shellQuote: false
- id: output_basename
  doc: |-
    Recommand use the task ID in the url above as output file prefix. 
    For example 598b5c92-cb1d-49b2-8030-e1aa3e9b9fde is the task ID from 
    	    https://cavatica.sbgenomics.com/u/yiran/variant-workbench-testing/tasks/598b5c92-cb1d-49b2-8030-e1aa3e9b9fde/#set-input-data
  type: string?
  default: gene-based-variant-filtering
  inputBinding:
    prefix: --output_basename
    position: 3
    shellQuote: false

outputs:
- id: variants_output
  type: File?
  outputBinding:
    glob: '*.tsv.gz'

baseCommand:
- tar
- -xvf
arguments:
- position: 1
  valueFrom: |-
    $(inputs.dbnsfp_annovar.path) && tar -xvf $(inputs.hgmd_var.path)
  shellQuote: false
- position: 2
  valueFrom: |-
    && python Gene_based_variant_filtering_vcfinput_allgene.py --dbnsfp ./$(inputs.dbnsfp_annovar.nameroot.replace(".tar", ""))/ --hgmd_var ./$(inputs.hgmd_var.nameroot.replace(".tar", ""))/ 
  shellQuote: false