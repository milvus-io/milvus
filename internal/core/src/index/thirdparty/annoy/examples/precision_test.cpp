/*
 * precision_test.cpp

 *
 *  Created on: Jul 13, 2016
 *      Author: Claudio Sanhueza
 *      Contact: csanhuezalobos@gmail.com
 */

#include <iostream>
#include <iomanip>
#include "../src/kissrandom.h"
#include "../src/annoylib.h"
#include <chrono>
#include <algorithm>
#include <map>
#include <random>


int precision(int f=40, int n=1000000){
	std::chrono::high_resolution_clock::time_point t_start, t_end;

	std::default_random_engine generator;
	std::normal_distribution<double> distribution(0.0, 1.0);

	//******************************************************
	//Building the tree
	AnnoyIndex<int, double, Angular, Kiss32Random> t = AnnoyIndex<int, double, Angular, Kiss32Random>(f);

	std::cout << "Building index ... be patient !!" << std::endl;
	std::cout << "\"Trees that are slow to grow bear the best fruit\" (Moliere)" << std::endl;



	for(int i=0; i<n; ++i){
		double *vec = (double *) malloc( f * sizeof(double) );

		for(int z=0; z<f; ++z){
			vec[z] = (distribution(generator));
		}

		t.add_item(i, vec);

		std::cout << "Loading objects ...\t object: "<< i+1 << "\tProgress:"<< std::fixed << std::setprecision(2) << (double) i / (double)(n + 1) * 100 << "%\r";

	}
	std::cout << std::endl;
	std::cout << "Building index num_trees = 2 * num_features ...";
	t_start = std::chrono::high_resolution_clock::now();
	t.build(2 * f);
	t_end = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::seconds>( t_end - t_start ).count();
	std::cout << " Done in "<< duration << " secs." << std::endl;


	std::cout << "Saving index ...";
	t.save("precision.tree");
	std::cout << " Done" << std::endl;



	//******************************************************
	std::vector<int> limits = {10, 100, 1000, 10000};
	int K=10;
	int prec_n = 1000;

	std::map<int, double> prec_sum;
	std::map<int, double> time_sum;
	std::vector<int> closest;

	//init precision and timers map
	for(std::vector<int>::iterator it = limits.begin(); it!=limits.end(); ++it){
		prec_sum[(*it)] = 0.0;
		time_sum[(*it)] = 0.0;
	}

	// doing the work
	for(int i=0; i<prec_n; ++i){

		//select a random node
		int j = rand() % n;

		std::cout << "finding nbs for " << j << std::endl;

		// getting the K closest
		t.get_nns_by_item(j, K, n, &closest, nullptr);

		std::vector<int> toplist;
		std::vector<int> intersection;

		for(std::vector<int>::iterator limit = limits.begin(); limit!=limits.end(); ++limit){

			t_start = std::chrono::high_resolution_clock::now();
			t.get_nns_by_item(j, (*limit), (size_t) -1, &toplist, nullptr); //search_k defaults to "n_trees * n" if not provided.
			t_end = std::chrono::high_resolution_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>( t_end - t_start ).count();

			//intersecting results
			std::sort(closest.begin(), closest.end(), std::less<int>());
			std::sort(toplist.begin(), toplist.end(), std::less<int>());
			intersection.resize(std::max(closest.size(), toplist.size()));
			std::vector<int>::iterator it_set = std::set_intersection(closest.begin(), closest.end(), toplist.begin(), toplist.end(), intersection.begin());
			intersection.resize(it_set-intersection.begin());

			// storing metrics
			int found = intersection.size();
			double hitrate = found / (double) K;
			prec_sum[(*limit)] += hitrate;

			time_sum[(*limit)] += duration;


			//deallocate memory
			vector<int>().swap(intersection);
			vector<int>().swap(toplist);
		}

		//print resulting metrics
		for(std::vector<int>::iterator limit = limits.begin(); limit!=limits.end(); ++limit){
			std::cout << "limit: " << (*limit) << "\tprecision: "<< std::fixed << std::setprecision(2) << (100.0 * prec_sum[(*limit)] / (i + 1)) << "% \tavg. time: "<< std::fixed<< std::setprecision(6) << (time_sum[(*limit)] / (i + 1)) * 1e-04 << "s" << std::endl;
		}

		closest.clear(); vector<int>().swap(closest);

	}

	std::cout << "\nDone" << std::endl;
	return 0;
}


void help(){
	std::cout << "Annoy Precision C++ example" << std::endl;
	std::cout << "Usage:" << std::endl;
	std::cout << "(default)		./precision" << std::endl;
	std::cout << "(using parameters)	./precision num_features num_nodes" << std::endl;
	std::cout << std::endl;
}

void feedback(int f, int n){
	std::cout<<"Runing precision example with:" << std::endl;
	std::cout<<"num. features: "<< f << std::endl;
	std::cout<<"num. nodes: "<< n << std::endl;
	std::cout << std::endl;
}


int main(int argc, char **argv) {
	int f, n;


	if(argc == 1){
		f = 40;
		n = 1000000;

		feedback(f,n);

		precision(40, 1000000);
	}
	else if(argc == 3){

		f = atoi(argv[1]);
		n = atoi(argv[2]);

		feedback(f,n);

		precision(f, n);
	}
	else {
		help();
		return EXIT_FAILURE;
	}


	return EXIT_SUCCESS;
}
