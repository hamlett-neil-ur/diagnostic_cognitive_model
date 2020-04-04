<p align="center">
<img height="90"  src="Watson Logo.png" > 

# Diagnostic Cognitive Model (DCM) feature of *IBM-Watson Classroom*.

</p> 

## Executive Summary/Abstract.

From 2014 - 2019, IBM attempted to expand its education-technology presence.  It launched a cloud-based platfrom called *Watson Classroom*. Personalization of instruction for each individual student was its vision.  

Mainstream education uses an operating model that resembles an assembly line [[F. J. Bramante, R. L. Colby]](https://amzn.to/2x5ydJ9).  What each student knows is presumed to be determined largely by the amount of time spent sitting in the classroom.  What and how quickly students learn actually varies due to a wide variety of factors.

*Watson Classroom* attempted to catch a the *Personalized-Learning* or *Competency-Based Education* wave of education reform [[U.S. Dept of Education]](https://www.ed.gov/oii-news/competency-based-learning-or-personalized-learning). Instead of measuring the amount of time spent in the classroom, this new method focuses on *evidence of learning*. Instruction and other learning activities are designed not just to transmit information.  They also elicit *evidence* that knowledge has actually been gained ([[C. A. Tomlinson, 2014]](http://www.ascd.org/publications/books/108029.aspx), [[G. Wiggins, J. McTighe, 2005]](http://www.ascd.org/Publications/Books/Overview/Understanding-by-Design-Expanded-2nd-Edition.aspx)).  Assembling the body of each student's evidence of learning leads to a complete, granular view of her strengths and weaknesses. *Watson Classroom* sought to assemble this perspective.


<img width="800" src="./ConceptOfOperations/170313 Conceptual Learner Model v2-5.png" align="center" >

This holistic, granular view of each student's *state of knowledge* — marketed as *Mastery* — was one of three key features contained in *Watson Classroom.*  The product also contained a high-level view of each student's academic status.  It was an attempt at reformulating a [*Student Information System*](https://en.wikipedia.org/wiki/Student_information_system), a student-data system most educational systems use.  It also contained a content-recommendation solution.  This used [natural-language processing](https://becominghuman.ai/a-simple-introduction-to-natural-language-processing-ea66a1747b32) to identify supplementary materials to reinforce individual students' weaknesses, identified by *Mastery*.

## File Directory.

|Artifact|Description|
|----|-----|
|ClientAdoption|Descriptions of offerings and materials to help client school districts adopt and derive value from *Watson Classroom*. As with most technologies, deriving maximum value depends on employment of certain business practices. These methods and consulting offerings helped clients get those in place.|
|ConceptOfOperations|Graphical and other materials illustrating the concept of operations underlying the *Mastery* feature.  It was based on fusing concepts from well-established practices in the educational literature.  These artifcats elaborate.| 
|CurriculumAlignmentAnalsisReporting|This contains artifacts — fusion of python and SQL technologies — used to analyze the degree of alignment of clients' curricula.  The *Mastery* feature required a well-aligned, digitized curriculum to function.|
|ProductionSubroutinesInPython|The algorithmic logic behind *Mastery* feature was realized in python.  This is a subset of that code.  It includes preliminary, original work to circumvent limitations in the open-source packages.|
|PrototypeSubroutinesInR|The *Mastery* logic was prototyped in R during the concept-exploration phase.  The logical flow largely resembles that instantiated in python.|
|180716_Algorithm_Description_Document_v0-1.pdf|This is an incomplete draft of an algorithm-description document.  It contains extensive elaboration on the motivation, rationale, and concept for operations.|
|190305 NMCE CADRE Abstract v1-0.pdf|A presentation about *Mastery* was accepted for the the National Council of Measurements in Ecucation [*Third annual NCME special conference on classroom assessment*](https://www.ncme.org/events/event-description?CalendarEventKey=8bc0ef9b-ddee-4dfa-bcfa-2d80460adee5&Home=%2Fmeetings%2Fupcoming-events). This is the abstract that was accepted.  The paper was withdrawn when *Watson Classroom* was discontinued.|

## Concept of Operations for the *Mastery* diagnostic cognitive model.

*Traditional* education largely uses time spent in clasroom as a proxy for how much a student learns.  In competency-based personalized learning, another measure is used:  Evidence of learning.  The amount each member of a given set of students acquires in a fixed time will vary significantly.  Evidence of learning — not time spent learning — is the basis for competency-based education.

Competency-based education requires an aligned curriculum.  The entire system must be thought of not only as an information-transfer system, but a system for measurement.  This is refered to alignment.

University of Pennsylvania researchers Andrew Porter and John Smithson described a widely-accepted framework for curriculum alignment [[A. C. Porter, J. L Smithson, 2001]](https://repository.upenn.edu/cgi/viewcontent.cgi?article=1058&context=cpre_researchreports).  Four facets of a curriculum must be considered.

The figure below depicts the curriculum-alignment point of view underlying the *Mastery* DCM.  When the first three facets — intended, enacted, and assessed — are in alignment, the *Mastery* DCM would attempt to infer what each student had learned, at a granular level. 

<img width="800" src="./ConceptOfOperations/170816 Curriculum Alignment from Porter.png" align="center" >

The table below elaborates on the curriculum-alignment framework by Porter and Smithson.  Other well-established frameworks for curriculum, instruction, and assessment (CIA) are fused in. The overall *Mastery* DCM framework was well-grounded in the best recommended principles in the education literature.

|Facet|Explanation|References|
|----|----|----|
|Intended Curriculum|Statement of knowledge learners are intended to acquire.  This is usually a policy statement specifying the structure of the curriculum and the content of courses, instructional units.|[[A. C. Porter, J. L Smithson, 2001]](https://repository.upenn.edu/cgi/viewcontent.cgi?article=1058&context=cpre_researchreports)|
|Enacted Curriculum|What is actually implemented in the learning environment.  This might include course syllabi, instructional plans, and the actual instruction.|[[C. A. Tomlinson, 2014]](http://www.ascd.org/publications/books/108029.aspx), [[G. Wiggins, J. McTighe, 2005]](http://www.ascd.org/Publications/Books/Overview/Understanding-by-Design-Expanded-2nd-Edition.aspx)|
|Assessed Curriculum|Learning that is actually *measured*. Measurements come in a variety of forms, from formal examinations, to projects, to class participation. |[[R. J. Mislevy, *et al*, 2002]](https://www.ets.org/Media/Research/pdf/RR-03-16.pdf), [[R. G. Almond, *et al*, 2015]](https://www.springer.com/gp/book/9781493921249)|
|Learned Curriculum|Actual knowledge, competency acquired. An individual's knowledge is never directly observable. Education measurements are indirect observations. The closely-fields of [*psychometrics*](https://en.wikipedia.org/wiki/Psychometrics) and [*cognitive doagnostics*](https://psycnet.apa.org/record/2007-14745-001) seek to infer the actual cognitive state from external measurements we can observe.|[[A. A. Rupp, *et al*, 2010]](https://amzn.to/39OgB1T), [[J. Leighton, M. Gierl, 2007]](https://amzn.to/34avJoQ)|