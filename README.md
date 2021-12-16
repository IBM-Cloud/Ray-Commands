# scale-out-cmd-ray

## Setup Ray Cluster

- Set up Python tools: Run `. ./setup_env.sh` in your shell.
  - This sets up a virtual Python environment and installs the required tools an d extensions (`lithopscloud` for generating Ray cluster configuration, `ray` library and client tool, `gen2-connector` for Ray for creating and managing IBM Cloud Gen2 VPC infrastructure for Ray).
  - Make sure to run the command as shown with prefaced *dot* and *space* to make the Python environment effective in your local shell.
- Generate a Ray cluster configuration for IBM Cloud Gen2 VPC: Run `lithopscloud` and provide the input interactively. 
  - Select `Ray Gen2` as the `compute backend`.
  - Specify an `IBM API KEY` of a user in IBM Cloud so that the tool can create a VPC. If needed [create a new API Key](https://cloud.ibm.com/iam/apikeys) first.
  - Select the `region` where the VPC environment should be created.
  - Instead of accepting `Create new VPC` you can also select an existing VPC if one exists for your user in the selected region.
  - Select the `availability zone` in your region where the VPC should be set up.
  - Select a `resource group` to which the infrastructure instances should be assigned to.
  - Name the new VPC appropriately and then select `yes` to let thge tool create and set up the VPC and generate a Ray cluster configuration for it.
  - **Take a note** of the path of the produced cluster yaml file **at the bottom** of the tool output.
- Get and prepare the cluster confiration: Run `./get_cluster_config.sh <path to above cluster config from lithopscloud>`.
  - This stores a file `cluster.yaml` in your local directory which has the required configuration to run the scale out Ray commands of this repository here.
- Start your Ray cluster: Run `ray up cluster.yaml`

## Tear Ray Cluster Down
When done running Scale-out Ray commands you should tear down your Ray cluster in your VPC to not unecessarily accumulate costs for it.

- Run `ray down cluster.yaml`

## Commands

### ray-cp

Run `./ray_cp.sh`. It asks you for the the input and output parameters for object storage endpoint, bucket, prefix as well as access key and secret key. You can streamline the usage of the tool by storing some of the parameter values in the following environment variables before you run the command:
- INPUT_ACCESS_KEY
- INPUT_SECRET_KEY
- OUTPUT_ACCESS_KEY
- OUTPUT_SECRET_KEY
- INPUT_ENDPOINT
- OUTPUT_ENDPOINT
- INPUT_BUCKET
- OUTPUT_BUCKET
- INPUT_PREFIX
- OUTPUT_PREFIX

