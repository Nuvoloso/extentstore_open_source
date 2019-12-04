# License

Copyright 2019 Tad Lebeck

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

# extentstore

To grab and build this workspace:

    dir=~/go/src/github.com/Nuvoloso

    mkdir -p $dir

    cd $dir

    git clone git@github.com:Nuvoloso/extentstore.git

    go get -u github.com/aws/aws-sdk-go-v2
    cd ~/go/src/github.com/aws/aws-sdk-go-v2
    make

A kontroller workspace cloned at the normal place is required. See
the kontroller instructions to set that up.

Make sure your credentials are set properly for aws.  The typical
way to is to have ~/.aws/config and ~/.aws/credentials set.

    export mybucket=nuvomike20180423
    export myregion=us-west-1

    cd ${dir}/extentstore/copy
    go build
    ./testit $mybucket $myregion
