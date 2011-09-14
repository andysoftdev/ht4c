HOW TO BUILD HT4C
=================

Building ht4c for Windows requires Microsoft Visual Studio 2010 Professional or better.

###Browse or get the source###

* Browse or download the source at [github](http://github.com/andysoftdev/ht4c).
  Download the latest sources by pressing the Downloads button and choosing
  Download.tar.gz or Download.zip.
  
* Or get the source from the repository, create a projects folder (the path must not
  contain any spaces) and use:

		mkdir hypertable
		cd hypertable
		git clone git://github.com/andysoftdev/ht4c.git


###Download and build Hypertable for Windows###

* See [How To build Hypertable for Windows](https://github.com/andysoftdev/ht4w/blob/windows/README.md).


###Build ht4c###

* Open the ht4c solution (...\\ht4c\\ht4c.sln) with Microsoft Visual Studio 2010 and build the solution configuration(s) or
  run Visual Studio Command Prompt and run:

		cd ...\ht4c
		msbuild ht4c.buildproj
  or for a complete rebuild

		cd ...\ht4c
		msbuild ht4c.buildproj /t:Clean;Make

