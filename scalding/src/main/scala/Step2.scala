package com.twitter.scalding.examples

import com.twitter.scalding._

class Step2(args : Args) extends Job(args) {
  
  val step1 = Tsv("/mnt/users/team7/step1.tsv", ('donor_id, 'country, 'gene_affected, 'mutation));

  // Group by gene. Keep only genes that have at least 500 people with a mutation on that gene.
  val grouped =       step1
         .groupBy('gene_affected, 'country) { _.size }
         .project('gene_affected, 'country, 'size)
         .filter('size) { size : Int =>  size > 10 }

  val gene_info = Tsv("/datasets/icgc/icgc8.gene_info.tsv", ('tax_id, 'gene_id, 'symbol, 'tail)) 
     // Filters out the header row
    .filter(('gene_id)) { gene_id : String => gene_id != "gene_id" }
    .project('gene_id, 'symbol)

 // Get a gene symbol instead of its ID
  val gene1 =  
     grouped.joinWithSmaller('gene_affected -> 'gene_id, gene_info)
          .project('symbol, 'country, 'size)
/*
  val gene2 = gene1.rename( ('symbol, 'country, 'size) -> ('symbol2, 'country2, 'size2))
  
  // Mega join!
  gene1.joinWithSmaller('country -> 'country2, gene2)
  */
    .write( Tsv("/mnt/users/team7/step2.tsv") )
}
