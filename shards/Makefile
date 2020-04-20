HOST=$(or $(host),127.0.0.1)
PORT=$(or $(port),19530)

build:
	docker build --network=host -t milvusdb/mishards .
push:
	docker push milvusdb/mishards
pull:
	docker pull milvusdb/mishards
deploy: clean_deploy
	cd all_in_one && docker-compose -f all_in_one.yml up -d && cd -
clean_deploy:
	cd all_in_one && docker-compose -f all_in_one.yml down && cd -
probe_deploy:
	docker run --rm --name probe --net=host milvusdb/mishards /bin/bash -c "python all_in_one/probe_test.py"
deploy_m: clean_deploy_m
	cd all_in_one_with_mysql && docker-compose -f all_in_one.yml up -d && cd -
clean_deploy_m:
	cd all_in_one_with_mysql && docker-compose -f all_in_one.yml down && cd -
probe_deploy_m:
	docker run --rm --name probe --net=host milvusdb/mishards /bin/bash -c "python all_in_one_with_mysql/probe_test.py"
cluster:
	cd kubernetes_demo;./start.sh baseup;sleep 10;./start.sh appup;cd -
clean_cluster:
	cd kubernetes_demo;./start.sh cleanup;cd -
cluster_status:
	kubectl get pods -n milvus -o wide
probe_cluster:
	@echo
	$(shell kubectl get service -n mishards | grep mishards-proxy-servers | awk {'print $$4,$$5'} | awk -F"[: ]" {'print "docker run --rm --name probe --net=host milvusdb/mishards /bin/bash -c \"python all_in_one/probe_test.py --port="$$2" --host="$$1"\""'})
probe:
	docker run --rm --name probe --net=host milvusdb/mishards /bin/bash -c "python all_in_one/probe_test.py --port=${PORT} --host=${HOST}"
clean_coverage:
	rm -rf cov_html
clean: clean_coverage clean_deploy clean_cluster clean_deploy_m
style:
	pycodestyle --config=.
coverage:
	pytest --cov-report html:cov_html --cov=mishards
test:
	pytest
