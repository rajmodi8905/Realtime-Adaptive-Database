# [cite_start]Assignment 4: Dashboard Enhancement, Performance Evaluation & Final System Packaging [cite: 1]

## [cite_start]1. Project Objective [cite: 2]

[cite_start]In the previous assignments, you have implemented the core components of a hybrid database framework including adaptive ingestion, metadata-driven storage decisions, automated query generation, and logical transaction coordination. [cite: 3] [cite_start]This assignment focuses on completing the system by enhancing the dashboard capabilities, evaluating the performance of the hybrid framework, and packaging the entire system into a deployable and well-documented software framework. [cite: 4] [cite_start]The objective is to demonstrate that the system can function as a complete logical database layer that provides a clean user interface while efficiently managing data across multiple storage backends. [cite: 5]

### [cite_start]Core Technical Pipeline [cite: 6]
[cite_start]The Assignment 4 implementation should extend the architecture developed in previous assignments with the following stages. [cite: 7]

* [cite_start]**Phase 1: Dashboard Enhancement** Extend the logical dashboard to support improved navigation, entity inspection, and query monitoring. [cite: 8]
* [cite_start]**Phase 2: Performance Benchmarking** Design experiments to measure system performance during data ingestion, query execution, and transaction coordination. [cite: 9]
* [cite_start]**Phase 3: Comparative Evaluation** Compare the performance of logical queries executed through the framework with direct queries on SQL or MongoDB. [cite: 10]
* [cite_start]**Phase 4: System Packaging** Prepare the final framework as a deployable software package with documentation and usage instructions. [cite: 11]

### Administrative Details
* [cite_start]**Deadline:** 6:00 PM, 18 April 2026 [cite: 12, 13]
* [cite_start]**Instructor:** Dr. Yogesh K. Meena [cite: 14]

---

## [cite_start]2. Dashboard Enhancement Requirements [cite: 18]

[cite_start]You must extend the dashboard implemented in Assignment 3 to provide a more comprehensive view of the logical database system. [cite: 20] [cite_start]The dashboard should continue to present data according to the logical schema while hiding all backend implementation details. [cite: 21] 

[cite_start]The dashboard should support: [cite: 22]
* [cite_start]Viewing active sessions [cite: 23]
* [cite_start]Listing logical entities within a session [cite: 24]
* [cite_start]Viewing instances of each entity [cite: 25]
* [cite_start]Inspecting field names and values of logical objects [cite: 26]
* [cite_start]Displaying results of executed logical queries [cite: 27]
* [cite_start]Viewing query execution history [cite: 28]

> [cite_start]**Constraint:** The interface must not reveal backend-specific details such as SQL tables, MongoDB collections, indexing strategies, or schema placement decisions. [cite: 29]

---

## [cite_start]3. Performance Evaluation [cite: 30]

[cite_start]You must design experiments to evaluate the performance of the hybrid database framework. [cite: 31] [cite_start]The performance analysis should consider: [cite: 31]
* [cite_start]Data ingestion latency [cite: 32]
* [cite_start]Logical query response time [cite: 33]
* [cite_start]Metadata lookup overhead [cite: 34]
* [cite_start]Transaction coordination overhead across SQL and MongoDB [cite: 35]

[cite_start]The experiments should collect metrics such as: [cite: 36]
* [cite_start]Average query latency [cite: 37]
* [cite_start]Throughput (operations per second) [cite: 38]
* [cite_start]Distribution of data across storage backends [cite: 39]

[cite_start]Students should analyse how the abstraction layer affects system performance. [cite: 40]

---

## [cite_start]4. Comparative Analysis [cite: 41]

[cite_start]You must compare the performance of the hybrid framework with direct database access. [cite: 42] [cite_start]The goal of this comparison is to understand the trade-offs introduced by the logical abstraction layer. [cite: 43] 

[cite_start]Students should design experiments comparing: [cite: 44]
* [cite_start]Retrieving user records through the logical query interface vs direct SQL queries [cite: 45]
* [cite_start]Accessing nested documents using the framework vs direct MongoDB queries [cite: 45]
* [cite_start]Updating records across multiple entities [cite: 45]

[cite_start]The comparison should measure metrics such as: [cite: 46]
* [cite_start]Query latency [cite: 48]
* [cite_start]Update latency [cite: 49]
* [cite_start]System throughput [cite: 50]
* [cite_start]Query processing overhead introduced by the framework [cite: 51]

[cite_start]Results should be presented using appropriate visualizations such as: [cite: 52]
* [cite_start]Bar charts comparing query latency [cite: 53]
* [cite_start]Line graphs showing throughput under increasing workload [cite: 54]
* [cite_start]Tables summarizing performance metrics [cite: 55]

[cite_start]Students should interpret the results and discuss scenarios where the logical abstraction introduces overhead as well as scenarios where it simplifies application development and improves data accessibility. [cite: 57]

---

## [cite_start]5. Final System Packaging [cite: 58]

[cite_start]You must prepare the entire system as a complete and reproducible software package. [cite: 59] [cite_start]The final system should include: [cite: 59]
* [cite_start]Source code repository (GitHub) [cite: 60]
* [cite_start]Setup instructions for dependencies [cite: 61]
* [cite_start]Instructions to configure SQL and MongoDB backends [cite: 62]
* [cite_start]Instructions to run the ingestion API [cite: 63]
* [cite_start]Instructions to run the logical query interface [cite: 64]
* [cite_start]Instructions to launch the dashboard [cite: 65]

[cite_start]The system should be organized so that another user can install and run the framework with minimal configuration effort. [cite: 66]

---

## [cite_start]6. Deliverables [cite: 67]

[cite_start]The assignment submission must include: [cite: 68]
* [cite_start]A single report: `group_name_final_report.pdf` [cite: 69]
* [cite_start]A short demonstration video [cite: 70]

[cite_start]**Report Requirements:** [cite: 71]
[cite_start]The first page must include: [cite: 72]
* [cite_start]GitHub repository link [cite: 73]
* [cite_start]Video demonstration link [cite: 74]
* [cite_start]Description of dashboard enhancements [cite: 75]
* [cite_start]Performance evaluation experiments [cite: 76]
* [cite_start]Comparative analysis results [cite: 77]
* [cite_start]Discussion of system limitations [cite: 78]

---

## [cite_start]7. Marking Criteria [cite: 80]

[cite_start]The following table details the marking criteria: [cite: 81]

| Criterion | Focus Area |
| :--- | :--- |
| Dashboard Enhancement | Usability and logical data presentation |
| Performance Evaluation | Quality of benchmarking experiments |
| Comparative Analysis | Understanding of abstraction vs performance trade-offs |
| System Packaging | Completeness and reproducibility of system setup |
| Report Quality | Technical clarity and explanation of experiments |

---

## [cite_start]8. Conclusion [cite: 83]

[cite_start]This assignment completes the development of the hybrid database framework by evaluating its usability, performance, and deployability. [cite: 84] [cite_start]The final system should demonstrate how logical abstraction and metadata-driven architectures can enable flexible and scalable data management across multiple database backends. [cite: 85]

> [cite_start]**Course Information:** CS 432 - Databases (Course Project / Assignment 4 / Track 2), Semester II (2025-2026), April 6, 2026. [cite: 15, 17, 86] [cite_start]Indian Institute of Technology, Gandhinagar. [cite: 15, 86] [cite_start]All rights reserved. [cite: 16, 87]