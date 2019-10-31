build:
	docker build --network=host -t milvusdb/mishards .
push:
	docker push milvusdb/mishards
pull:
	docker pull milvusdb/mishards
deploy:
	cd all_in_one && docker-compose -f all_in_one.yml up -d && cd -
clean_deploy:
	cd all_in_one && docker-compose -f all_in_one.yml down && cd -
clean_coverage:
	rm -rf cov_html
clean: clean_coverage clean_deploy
style:
	pycodestyle --config=.
coverage:
	pytest --cov-report html:cov_html --cov=mishards
test:
	pytest
