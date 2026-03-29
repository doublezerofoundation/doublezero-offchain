.PHONY: e2e-build e2e-build-debug e2e-test e2e-test-debug e2e-test-nobuild e2e-test-keep e2e-test-keep-nobuild e2e-test-cleanup e2e-devnet-start e2e-devnet-stop e2e-devnet-destroy

e2e-build:
	$(MAKE) -C e2e build

e2e-build-debug:
	$(MAKE) -C e2e build-debug

e2e-test:
	$(MAKE) -C e2e test RUN=$(RUN)

e2e-test-debug:
	$(MAKE) -C e2e test-debug RUN=$(RUN)

e2e-test-nobuild:
	$(MAKE) -C e2e test-nobuild RUN=$(RUN)

e2e-test-keep:
	$(MAKE) -C e2e test-keep RUN=$(RUN)

e2e-test-keep-nobuild:
	$(MAKE) -C e2e test-keep-nobuild RUN=$(RUN)

e2e-test-cleanup:
	$(MAKE) -C e2e test-cleanup

e2e-devnet-start:
	$(MAKE) -C e2e devnet-start ARGS=$(ARGS)

e2e-devnet-stop:
	$(MAKE) -C e2e devnet-stop

e2e-devnet-destroy:
	$(MAKE) -C e2e devnet-destroy
