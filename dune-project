(lang dune 3.13)

(name 1brc)

(generate_opam_files true)

(authors "KFoxder")

(package
 (name 1brc)
 (depends ocaml dune (core ( >= 0.16.2)) (base ( >= 0.16.2)) domainslib)
 )
