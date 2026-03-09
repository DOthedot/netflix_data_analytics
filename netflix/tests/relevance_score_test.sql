-- Custom test: asserts no row in fct_genome_scores has any NULL column.
-- A result set with rows = test failure.
{{ full_table_null_check(ref('fct_genome_scores')) }}
