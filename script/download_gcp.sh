if [ $(ls -l | grep 'download_gcp.sh' | wc -l) = "0" ]; then
	echo 'cd to the directory before sourcing!'
else
	echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
	sudo apt update
	sudo apt-get install -y apt-transport-https ca-certificates gnupg golang wondershaper
	curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
	sudo apt-get update && sudo apt-get install -y google-cloud-sdk
	sudo tc qdisc add dev eno1 root tbf rate 400mbit burst 10kb latency 200ms peakrate 600mbit minburst 1540
	gsutil -m cp -r gs://clusterdata_2019_a/ ../data
	echo 'export GOPATH=~/go' >> ~/.bashrc
	export GOPATH=~/go
	mkdir ~/go
	ln -sf $(pwd)/.. ~/go/google_cluster_project
	go get google.golang.org/protobuf/encoding/protojson

fi
