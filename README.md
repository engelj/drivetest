drivetest
=========
Test hard disks / NAS for performance and errors with near-use random access and/or get more confidence by advancing through infant failures/mortality ( http://en.wikipedia.org/wiki/Bathtub_curve ).

The idea is to create reasonable file contents, create checksums for it, (manually) stress the disk further and repeat checking checksums and/or evaluate S.M.A.R.T ( http://en.wikipedia.org/wiki/S.M.A.R.T.) testing/logs.

All tests only use free disk space and never delete existing data outside a given root directory. Of course, if the disk/system breaks, loss of any data must be taken into account.

One could think of using up yet unneeded space with test files and regularly check for wear out. With any luck, defects will also show up in test data whilst /before in productive data where it might be
difficult to discover. This goes beyound the fact that hard disk firmware and OS filesystem logic might catch such errors anyway.

It was tested on Linux (Ubuntu 10.04.2 LTS) but should work on Windows and any flavour of Linux/Unix. Needs at least Python/2.5.

As a side effect (if you let it fill up) your hard disk gets pretty erased and just looks (much too) innocent.

Feedback/improvements welcome! Ask for updates. USE AT YOUR OWN RISK!

Warning
=======
BE AWARE THAT IT COULD KILL YOUR AGED DISK INCLUDING DATA! BACK IT UP!
FIRST!

Usage Examples
--------------

*Create Files*

Create an unlimited number of files of random size up to ~1G bytes with random contents under the directory /mnt/drivetest reserving ~10G free space; After creation of a file the speed is printed, press Ctrl-C to stop; at last the average speed is printed:

`python drivetest.py createfiles --root /mnt --max_size 1e9 --min_free 10e9`

*Create Checksums*

Create MD5 checksums for the above files and rename them to names including the checksum; it can be called several times as it ignores already checksummed/renamed files; you could run it in parallel to the 'createfiles' call to simulate concurrent read/write access and to speed up overall till completion of a wanted set of checksummed files:

`python drivetest.py createchecksums --root /mnt`

*Check Checksums*

Look for all files which are checksummed and compare the file contents with the checksum being part of the filename:

`python drivetest.py checkchecksums --root /mnt`

