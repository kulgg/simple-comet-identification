nextflow.enable.dsl = 2

// required arguments
params.cometBin = ""
params.cometParams = ""
params.fasta = ""
params.mzmlDir = ""
params.resultsDir = ""
params.fdrThreshold = 0.05
params.keepDecoys = 0
params.macpepdbWebApi = ""

// Makes sure the txt output is active
process adjust_comet_params {
    input:
    path comet_param_file

    output:
    path "comet.params.adjusted"

    """
    cat ${comet_param_file} | sed 's/output_txtfile = 0/output_txtfile = 1/g' > comet.params.adjusted
    """
}

process search {
    maxForks 1

    input:
    path comet_param_file
    path fasta_file
    path mzml

    output:
    path "*.tsv"
    """

    ${params.cometBin} -P${comet_param_file} -D${fasta_file} ${mzml}

    for file in *.txt; do
        mv -- "\$file" "\$(basename \$file .txt).psms.tsv"
    done
    """
}


process filter {
    input: 
    path "*"

    output:
    path "*.tsv", includeInputs: true
    
    """
    psm_file=(*.tsv)
    fdr_filter.py \${psm_file} --fdr ${params.fdrThreshold} ${params.keepDecoys != 0 ? '--keep-decoys' : ''}
    """
}

process annotate {
    publishDir "${params.resultsDir}/psms", mode: 'copy'

    input:
    path "*"

    output:
    path "*.tsv", includeInputs: true

    """
    annotate.py *.tsv ${params.macpepdbWebApi}
    """
}


workflow() {
    mzmls = Channel.fromPath(params.mzmlDir + "/*.{mzML,mzml}")
    comet_param_file = Channel.fromPath(params.cometParams).first()
    comet_param_file_adjusted = adjust_comet_params(comet_param_file)
    fasta_file = Channel.fromPath(params.fasta).first()

    psms_files = search(comet_param_file_adjusted, fasta_file, mzmls)
    filtered_psm_files = filter(psms_files)
    annotate(filtered_psm_files)
}