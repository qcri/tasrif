#!/bin/bash

usage() { echo "Usage: $0 [-v <version number in x.y.z format>] [-m <version description>]" 1>&2; exit 1; }

while getopts ":v:m:" opt;
do
  case $opt in
    v) 
        tasrif_version=${OPTARG}
        ;;
    m)
        tasrif_version_description=${OPTARG}
        ;;
    *)
        usage
  esac
done
shift $((OPTIND-1))

if [ -z "${tasrif_version}" ] || [ -z "${tasrif_version_description}" ]; then
    usage
fi

echo "TASRIF_VERSION = ${tasrif_version}"
echo "TASRIF_VERSION_DESCRIPTION = ${tasrif_version_description}"

echo "Publishing v${tasrif_version} ..."

echo -n "Step 1 of 9: Cleanup up previous builds.."
rm -rf build dist tasrif.egg-info

if ! [ -z "$(ls -A dist)" ]; then
    echo "FAILED! Could not clean up previous builds. Please clean manually.."
    exit 1
else
    echo "Done!"
fi

echo -n "Step 2 of 9: Update twine.."
python3 -m pip install --upgrade twine
echo "Done!"

echo -n "Step 3 of 8: Building package.."
python3 -m build

if [ -z "$(ls -A dist)" ]; then
    echo "FAILED! Failed building wheeels!"
    exit 1
else
    echo "Done!"
fi

echo -n "Step 4 of 9: Uploading to pypi.."
echo $TWINE_USERNAME 
echo $TWINE_PASSWORD
python3 -m twine upload --repository pypi  dist/*  
echo "Done!"

echo -n "Step 5 of 9: Building docker image of Tasrif.."
docker build . --file Dockerfile --tag tasrif/tasrif:${tasrif_version}
docker login -u tasrif -p $DOCKER_PWD
docker push  tasrif/tasrif:${tasrif_version}
echo "Done!"

echo "Step 6 of 9: Generating documentation.."
docker run -entrypoint "/bin/bash" -v $PWD/generated/docs/build:/home/docs/build \
            tasrif:${tasrif_version} \
            -c "cd docs; make html"
echo "Done!"

echo "Step 7 of 9: Donwload azcopy.."
wget -O azcopy_v10.tar.gz https://aka.ms/downloadazcopy-v10-linux && tar -xf azcopy_v10.tar.gz --strip-components=1
echo "Done!"

echo

echo "Step 8 of 9: Upload documentation files.."
./azcopy cp "generated/docs/build/html" "https://tasrifweb.blob.core.windows.net/\$web$AZURE_SAS" --recursive=true
echo "Done!"

echo -n "Step 9 of 9: Tagging version on Git.."
git tag "v${tasrif_version}" -m "${tasrif_version} - ${tasrif_version_description}"
git push --tags
echo "Done!"
