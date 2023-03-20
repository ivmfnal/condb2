FILES = ConDB.py __init__.py client.py csv.py dbdig.py http_client.py py3.py signature.py timelib.py version.py

build: lib bin
	
lib: $(LIBDIR)
	cp $(FILES) $(LIBDIR)

bin: $(BINDIR)
	cp ui/condb $(BINDIR)
	chmod +x $(BINDIR)/condb

$(LIBDIR):
	mkdir -p $@

$(BINDIR):
	mkdir -p $@

