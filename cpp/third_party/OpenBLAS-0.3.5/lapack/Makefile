TOPDIR	= ..
include ../Makefile.system

#SUBDIRS	= laswp getf2 getrf potf2 potrf lauu2 lauum trti2 trtri getrs
SUBDIRS	= getrf getf2 laswp getrs potrf potf2 lauu2 lauum trti2 trtri

FLAMEDIRS = laswp getf2 potf2 lauu2 trti2

libs:
	@for d in $(SUBDIRS) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done

prof:
	@for d in $(SUBDIRS) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	  (cd $$d; $(MAKE) prof) ; \
	fi; \
	done

flame:
	@for d in $(FLAMEDIRS) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d libs || exit 1 ; \
	fi; \
	done

hpl:

hpl_p:

clean ::
	@for d in $(SUBDIRS) tpp ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done

