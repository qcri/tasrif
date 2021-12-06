## How to contribute code

There are 3 ways you can contribute to Tasrif:
- Fixing bugs/outstanding issues with the existing code;
- Implementing new operators;
- Contributing to the examples or to the documentation;

Follow these steps to submit your code contribution.

### Step 1. Open an issue

If the changes are minor (simple bug fix or documentation fix), then feel free
to open a PR without discussion. Otherwise, we recommend opening an issue (if one doesn't already
exist) and discuss your proposed changes. This way, we can give you feedback
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

```python
git clone git@github.com:qcri/tasrif.git
cd tasrif
git remote add upstream https://github.com/<your-github-username>/tasrif.git
```

Create a new branch to hold your development changes:

```python
git checkout -b branch-name
```

Do not work on the master branch.

Set up a development environment by running the following command in a virtual environment:

```python
python3 -m venv tasrif-env
source tasrif-env/bin/activate
(tasrif-env) pip install --upgrade pip
(tasrif-env) cd tasrif
(tasrif-env) pip install -e .
```

Make sure you have `qa-requirements.txt` and `requirements.txt` installed, so you can use `pylint` and `darglint` modules

```python
(tasrif-env) pip install -r requirements.txt
(tasrif-env) pip install -r qa-requirements.txt
```

### Step 2. Make code changes

Make sure that your environment is set from the previous section. You can create your own custom operator by following the [Custom Operators](https://tasrif.qcri.org/custom-operators.html) section in the tutorial.

After making the changes, check if you have pylint or darglint errors. The [CI pipeline](https://github.com/qcrisw/tasrif/actions) checks those errors by default. However, it is preferable to check the errors locally by running

```python
(tasrif-env) pylint --rcfile=.pylintrc path/to/changed/file
```

or in docker

```python
docker build . -t tasrif
docker run -it tasrif:latest pylint tasrif
```

And darglint by

```python
(tasrif-env) darglint -s google path/to/changed/file
```


### Step 3. Create a pull request

Once the change is ready, open a pull request from your branch in your fork to
the master branch in [Tasrif](https://github.com/qcri/tasrif).

### Step 4. Code review

Before waiting for a reviewer to see the pull request, please look into the [CI pipeline](https://github.com/qcrisw/tasrif/actions) ran on your branch to see if there are any errors.

### Step 5. Merging

Once the pull request is approved, a `ready to pull` tag will be added to the
pull request. A team member will take care of the merging.

Here is an [example pull request](https://github.com/qcri/tasrif/pull/5)
for your reference.
