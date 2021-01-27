# How to contribute to CloudConductor (CC)?

Please follow this guide to contribute to the CC.

## Requirements
1. [Git]
2. [Github Account]

## Steps to follow
1. Download the [Git] on your machine.
2. Set up a [GitHub Account].
3. Clone the [CC] repository to your machine. Use the following command line:
```bash
git clone https://github.com/labdave/CloudConductor
```
4. Make a new branch from the `master` branch. Use the following command line:
```bash
git checkout -b feature_branch_name
```
5. Do all the development being on your feature branch.
6. Test all the changes.
7. Commit all the changes.
8. If the `remote` `feature branch` was not created before. Push the changes to the [CC] `GitHub` repo using the following 
   command line:
```bash
git push -u origin feature_branch_name
```
otherwise, use the following command line:
```bash
git push origin feature_branch_name
```
9. Create a pull request on [CC] `GitHub` repo.

Someone from the CC development team will review your pull request and get back to you soon if there are any suggestions; otherwise, the changes will get merged into the `master` branch.

For any questions, please email the CloudConductor team:
1. [Tushar Dave]
2. [Clay Parker]

[Git]: https://git-scm.com/
[Github Account]: https://github.com/
[CC]: https://github.com/labdave/CloudConductor
[Tushar Dave]: mailto:tushar.dave@duke.edu
[Clay Parker]: mailto:clay.parker@ddb.bio
