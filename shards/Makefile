build:
	docker build --network=host -t milvusdb/mishards .
push:
	docker push milvusdb/mishards
pull:
	docker pull milvusdb/mishards
deploy:
	cd all_in_one && docker-compose -f all_in_one.yml up -d && cd -
clean:
	rm -rf cov_html
	cd all_in_one && docker-compose -f all_in_one.yml down && cd -
check_style:
	pycodestyle --config=.
make test:
	pytest --cov-report html:cov_html --cov=mishards
