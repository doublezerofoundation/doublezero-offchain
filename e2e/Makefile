.PHONY: build build-debug test test-debug test-nobuild test-keep test-keep-nobuild test-cleanup devnet-start devnet-stop devnet-destroy

build:
	go run -tags=e2e ./cmd/devnet build

build-debug:
	go run -tags=e2e ./cmd/devnet build -v

test:
	go test -tags=e2e -timeout=30m -v -count=1 $(if $(RUN),-run $(RUN)) .

test-debug:
	DEBUG=1 go test -tags=e2e -timeout=30m -v -count=1 $(if $(RUN),-run $(RUN)) .

test-nobuild:
	OFFCHAIN_E2E_NO_BUILD=1 go test -tags=e2e -timeout=30m -v -count=1 $(if $(RUN),-run $(RUN)) .

test-keep:
	TESTCONTAINERS_RYUK_DISABLED=true go test -tags=e2e -timeout=30m -v -count=1 $(if $(RUN),-run $(RUN)) .

test-keep-nobuild:
	TESTCONTAINERS_RYUK_DISABLED=true OFFCHAIN_E2E_NO_BUILD=1 go test -tags=e2e -timeout=30m -v -count=1 $(if $(RUN),-run $(RUN)) .

test-cleanup:
	@echo "Removing containers with label offchain.doublezero..."
	@docker rm -f $$(docker ps -aq --filter label=offchain.doublezero) 2>/dev/null || true
	@echo "Removing networks with label offchain.doublezero..."
	@docker network rm $$(docker network ls -q --filter label=offchain.doublezero) 2>/dev/null || true

devnet-start:
	go run -tags=e2e ./cmd/devnet start $(ARGS)

devnet-stop:
	go run -tags=e2e ./cmd/devnet stop

devnet-destroy:
	go run -tags=e2e ./cmd/devnet destroy
