HOW TO BUILD ht4c
=================

Building ht4c from source requires at least Microsoft Visual Studio 2010 Professional.


###Browse through or download the ht4c source###

* Browse through or download the ht4c source from [github](http://github.com/andysoftdev/ht4c).
  To download the latest sources press the 'Downloads' button and choose one of the following files, either the
  Download.tar.gz or the Download.zip.
  
* Alternatively, get the source from the repository, create a projects folder (the path must not contain any spaces) and use:

		mkdir hypertable
		cd hypertable
		git clone git://github.com/andysoftdev/ht4c.git


###Download and build Hypertable for Windows###

* See [How To build Hypertable for Windows](https://github.com/andysoftdev/ht4w/blob/windows/README.md).


###Build ht4c###

* Open the ht4c solution (ht4c\ht4c.sln) with Microsoft Visual Studio 2010 and build its configurations. Alternatively, open the Visual Studio command prompt and type:

		cd ht4c
		msbuild ht4c.buildproj
  or, for a complete rebuild, type

		cd ht4c
		msbuild ht4c.buildproj /t:Clean;Make

