BLDROOT = $(HOME)/build/condb2
SRVDIR = $(BLDROOT)/server
LIBDIR = $(BLDROOT)/condb
BINDIR = $(BLDROOT)/bin
TARDIR = /tmp/$(USER)
CLTAR = $(TARDIR)/condb2_client_$(VERSION).tar
SRVTAR = $(TARDIR)/condb2_server_$(VERSION).tar

all:
	make VERSION=`python condb/version.py` all_with_version_defined

all_with_version_defined: tars

tars:	clean build_ $(TARDIR)
	cd $(BLDROOT);  tar cf $(CLTAR) bin condb
	cd $(BLDROOT);	tar cf $(SRVTAR) server condb
	@echo
	@echo Client tarfile ........... $(CLTAR)
	@echo Server tarfile ........... $(SRVTAR)
	@echo
    
build_:  $(SRVROOT) $(CLROOT)
	cd condb; make VERSION=$(VERSION) LIBDIR=$(LIBDIR) BINDIR=$(BINDIR) build
	cd server; make VERSION=$(VERSION) SRVDIR=$(SRVDIR) build

clean:
	rm -rf $(BLDROOT) $(CLTAR) $(SRVTAR)
    
$(SRVROOT):
	mkdir -p $@

$(CLROOT):
	mkdir -p $@
    
$(TARDIR):
	mkdir -p $@
    
    
   
	