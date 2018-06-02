# KBucket

System for sharing data for scientific research

Developers: Jeremy Magland, with contributions from Dylan Simon and Alex Morley

Flatiron Institute, a divison of Simons Foundation

## Philosophy

In many scientific fields it is essential to be able to exchange, share, archive, and publish experimental data. However, raw data files needed to reproduce results can be enormous. The philosophy of KBucket is to separate file contents from the files themselves, and to host data content where it most naturally lives, that is, in the lab where it was generated. By replacing large data files by tiny universal pointers called PRV files, we open up diverse ways for sharing datasets, studies, and results.

For example, sometimes it is useful to email data to a collaborator, other times it's nice to post it to slack, or to maintain a study on github, or share a directory structure on dropbox, or google drive, or post it to a website or forum. However, if the files are many Gigabytes or even Terabytes, many of those options become unfeasible without a system like KBucket.

## Objectives

In this project we set out to accomplish the following objectives.

* Easy to install and use from workstations, servers, and clusters alike (Linux or Mac), even behind firewalls.

* Simple and intuitive to share a directory of data. As simple as

```
kbucket-share /path/to/data/directory
```

* Easy to create tiny PRV files (universal pointers) that can then be shared in numerous ways. As simple as

```
ml-prv-create /some/data/file.dat file.dat.prv
```

* Any recipient (anywhere on the internet) can then use the PRV file to do a variety of things:

  - Download the original file in its entirety
  
  - Download just a part of the file
  
  - Use the PRV file in processing pipelines (python, javascript, matlab, etc) as though the file was the original
  
  - Visualize and explore the original data using web or desktop user interfaces that interactively load portions of the file
  
* Ability to publish rich, interactive web views of huge datasets

* Data can be hosted in the lab where it was generated, but also mirrored in different locations, depending on the application

[IN PROGRESS]
