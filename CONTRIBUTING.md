## How to contribute code

There are 3 ways you can contribute to Tasrif:
- Fixing bugs/outstanding issues with the existing code;
- Implementing new operators;
- Contributing to the examples or to the documentation;

Follow these steps to submit your code contribution.

### Step 1. Open an issue

Before making any changes, we recommend opening an issue (if one doesn't already
exist) and discussing your proposed changes. This way, we can give you feedback
and validate the proposed changes. Here is a template


Motivation first:

Is it related to a problem/frustration with the library? If so, please explain why. Providing a code snippet that demonstrates the problem is best.
Is it related to something you would need for a project?
Is it something you worked on and think could benefit the community?

Write a full paragraph describing the feature;
Provide a code snippet that demonstrates its future use;
In case this is related to a paper, please attach a link;
Attach any additional information (drawings, screenshots, etc.) you think may help.


Fork the repository by clicking on the 'Fork' button on the repository's page. This creates a copy of the code under your GitHub user account.

Clone your fork to your local disk, and add the base repository as a remote:

$ git clone git@github.com:<your Github handle>/tasrif.git
$ cd tasrif
$ git remote add upstream https://github.com/QCRI/tasrif.git

Create a new branch to hold your development changes:

$ git checkout -b a-descriptive-name-for-my-changes

Do not work on the master branch.

Set up a development environment by running the following command in a virtual environment:

$ MINIMAL=1 pip install -e .

To run the full test suite, you might need the additional dependency on datasets which requires a separate source install: (provide helpful Tasrif script to run tests on local or docker..)
TODO: add script to run tests in docker or virtualenv


If the changes are minor (simple bug fix or documentation fix), then feel free
to open a PR without discussion.




### Step 2. Make code changes

To make code changes, you need to fork the repository. You will need to setup a
development environment and run the unit tests. This is covered in section
"Setup environment".

### Step 3. Create a pull request

Once the change is ready, open a pull request from your branch in your fork to
the master branch in [qcri/tasrif](https://github.com/qcri/tasrif).

### Step 4. Code review

Before waiting for a reviewer to see the pull request, please look into the [CI pipeline](https://github.com/qcrisw/tasrif/actions) ran on your branch to see if there are any errors.

### Step 5. Merging

Once the pull request is approved, a `ready to pull` tag will be added to the
pull request. A team member will take care of the merging.

Here is an [example pull request](https://github.com/qcri/tasrif/pull/5)
for your reference.

## Setup environment

We provide two options for the development environment. One is to use our
Dockerfile, which builds into a container with the required dev tools. The other option is
to setup a local environment by installing the dev tools needed.

### Option 1: Use a Docker container


### Option 2: Setup a local environment


## Run tests

We use [Bazel](https://bazel.build/) to build and run the tests.


### Run a single test case

The best way to run a single test case is to comment out the rest of the test
cases in a file before running the test file.

### Run all tests

You can run all the tests locally by running the following command in the repo
root directory.

```
bazel test --test_timeout 300,450,1200,3600 --test_output=errors --keep_going --define=use_fast_cpp_protos=false --build_tests_only --build_tag_filters=-no_oss --test_tag_filters=-no_oss keras/...
```
