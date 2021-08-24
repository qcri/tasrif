.. Tasrif documentation master file, created by
   sphinx-quickstart on Mon Aug 23 16:17:33 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Tasrif's documentation!
==================================

**Date**: |today| **Version**: |release|

Introduction
============

Tasrif is a library for processing of eHealth data. It provides:

* DataReaders for reading eHealth datasets such as `MyHeartCounts`_, `SleepHealth`_
  and data from FitBit devices.
* A pipeline DSL for chaining together commonly used processing operations on
  time-series eHealth data, such as resampling, normalization, etc.

.. _MyHeartCounts: https://www.synapse.org/?source=post_page-------------------
  --------#!Synapse:syn11269541/wiki/
.. _SleepHealth: https://www.synapse.org/#!Synapse:syn18492837/wiki/

Packages
========

.. autopackagesummary:: tasrif
   :toctree: .
   :template: autosummary/package.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
