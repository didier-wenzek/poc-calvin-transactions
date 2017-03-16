all:
	ocamlbuild -use-ocamlfind single_node.native transaction_manager.native partition_server.native

clean:
	ocamlbuild -clean
