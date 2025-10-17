integration-test-core:
	docker compose run --rm storefront pnpm run test

integration-test-core-bench:
	docker compose run --rm storefront pnpm run bench