VERSION=$(shell grep -E 'VERSION ?=.*' pgsched.py | cut -d "'" -f 2)
DDIR=lbox-pgsched-$(VERSION)

all: dist

dist:
	mkdir -p $(DDIR)/sql
	ln pgsched.py init-script README $(DDIR)
	ln sql/pgsched.sql sql/uninstall_pgsched.sql $(DDIR)/sql
	tar -czf $(DDIR).tgz $(DDIR)
	rm -rf $(DDIR)
