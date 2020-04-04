<p align="center">
<img height="80"  src="Watson Logo.png" > 

# Diagnostic Cognitive Model (DCM) feature of *IBM-Watson Classroom*.

</p> 

## Executive Summary/Abstract.

From 2014 - 2019, IBM attempted to expand its education-technology presence.  It launched a cloud-based platfrom called *Watson Classroom*. Personalization of instruction for each individual student was its vision.  

Mainstream education uses an operating model that in some regards resembles an assembly line [[F. J. Bramante, R. L. Colby]](https://amzn.to/2x5ydJ9).  What each student knows is presumed to be determined largely by the amount of time spent sitting in the classroom.  What and how quickly students learn actually varies due to a wide variety of factors.

*Watson Classroom* attempted to catch a the *Personalized-Learning* or *Competency-Based Education* wave of education reform [[U.S. Dept of Education]](https://www.ed.gov/oii-news/competency-based-learning-or-personalized-learning). Instead of measuring the amount of time spent in the classroom, this new paradigm focuses on *evidence of learning*. Instruction and other learning activities are designed not just to transmit information.  They also elicit *evidence* that knowledge has actually been gained ([[C. A. Tomlinson, 2014]](http://www.ascd.org/publications/books/108029.aspx), [[G. Wiggins, J. McTighe, 2005]](http://www.ascd.org/Publications/Books/Overview/Understanding-by-Design-Expanded-2nd-Edition.aspx)).  Assembling the body of each student's evidence of learning leads to a complete, granular view of her strengths and weaknesses. *Watson Classroom* sought to assemble this perspective.

<img width="750" src="./ConceptOfOperations/170313 Conceptual Learner Model v2-5.png" align="right" >

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

## Concept of Operations for the *Mastery* DCM.

*Traditional* education largely uses time spent in clasroom as a proxy for how much a student learns.  In competency-based personalized learning, another measure is used:  Evidence of learning.  The amount each member of a given set of students acquires in a fixed time will vary significantly.  Evidence of learning — not time spent learning — is the basis for competency-based education.

Competency-based education requires an aligned curriculum.  The entire system must be thought of not only as an information-transfer system, but a system for measurement.  This is refered to alignment.

<img width="700" src="./ConceptOfOperations/170816 Curriculum Alignment from Porter.png" align="left" >

University of Pennsylvania researchers Andrew Porter and John Smithson described a widely-accepted framework for curriculum alignment [[A. C. Porter, J. L Smithson, 2001]](https://repository.upenn.edu/cgi/viewcontent.cgi?article=1058&context=cpre_researchreports).  Four facets of a curriculum must be considered.

The figure to the left depicts the curriculum-alignment point of view underlying the *Mastery* DCM.  When the first three facets — intended, enacted, and assessed — are in alignment, the *Mastery* DCM would attempt to infer what each student had learned, at a granular level. 

The table below elaborates on the curriculum-alignment framework by Porter and Smithson.  Other well-established frameworks for curriculum, instruction, and assessment (CIA) are fused in. The overall *Mastery* DCM framework was well-grounded in the best recommended principles in the education literature.

|Facet|Explanation|References|
|----|----|----|
|Intended Curriculum|Statement of knowledge learners are intended to acquire.  This is usually a policy statement specifying the structure of the curriculum and the content of courses, instructional units.|[[A. C. Porter, J. L Smithson, 2001]](https://repository.upenn.edu/cgi/viewcontent.cgi?article=1058&context=cpre_researchreports)|
|Enacted Curriculum|What is actually implemented in the learning environment.  This might include course syllabi, instructional plans, and the actual instruction.|[[C. A. Tomlinson, 2014]](http://www.ascd.org/publications/books/108029.aspx), [[G. Wiggins, J. McTighe, 2005]](http://www.ascd.org/Publications/Books/Overview/Understanding-by-Design-Expanded-2nd-Edition.aspx)|
|Assessed Curriculum|Learning that is actually *measured*. Measurements come in a variety of forms, from formal examinations, to projects, to class participation. |[[R. J. Mislevy, *et al*, 2002]](https://www.ets.org/Media/Research/pdf/RR-03-16.pdf), [[R. G. Almond, *et al*, 2015]](https://www.springer.com/gp/book/9781493921249)|
|Learned Curriculum|Actual knowledge, competency acquired. An individual's knowledge is never directly observable. Education measurements are indirect observations. The closely-fields of [*psychometrics*](https://en.wikipedia.org/wiki/Psychometrics) and [*cognitive doagnostics*](https://psycnet.apa.org/record/2007-14745-001) seek to infer the actual cognitive state from external measurements we can observe.|[[A. A. Rupp, *et al*, 2010]](https://amzn.to/39OgB1T), [[J. Leighton, M. Gierl, 2007]](https://amzn.to/34avJoQ)|

## Approach to *Mastery* DCM.

To paraphrase and extend [[R. G. Almond, R. J. Mislevy, *et al*, 2015]](https://www.springer.com/gp/book/9781493921249), "what a student knows after instruction is dependendent on what was known before, the quality of the instruction, and the extent of the student's engagement with the material".  This is a very Bayesian-sounding proposition.

Moreover, as was observed above, the an individual learner's state of knowledge is not directly observable. [[R. G. Almond, R. J. Mislevy, *et al*, 2015]](https://www.springer.com/gp/book/9781493921249) also explicitly states that.  The state of knowledge is a *latent* characteristic of the learner.

<img width="600" src="./ConceptOfOperations/190409 Phenomonology Model.png" align="right" >

The graphic to the right contains a phenomenology model.  In terms of measurement theory (e.g., [[S. Salicone, M. Prioli, 2018]](https://www.springer.com/us/book/9783319741376)), the *assessed curriculum* — what we can explicitly observe — is above the surface. We call this the *measurement domain*. 

The actual state of knowledge, the *measurand domain*, is below the surface. We attempt to infer the state of the *measurand* given directly-observed *measurements*.  Probability and statistics applies a particular term to this.  This is a *Latent-Variable* problem (e.g., [[J. C. Lohelin, 2009]](https://amzn.to/2xQbOzm)). These are generally solved using Bayesian techniqes.


Other factors also push us in the direction of a Bayiesian method for a DCM as opposed to a machine-learning approach.  First, edcuation measurements are in practice inconsistent and incomplete. Churn in student enrolment is considerable.  At any given time, a substantial plurality of students in a given class will not have complete assessments history.

Second, using test scores to infer other test scores is problematic.  The graphic above accentuates that. Educational measurements are at best noisy measurements of a latent variable.  Additionally, they are often incomplete. A given measurement may not capture all of the important dimensions [[D. S. Bhola, *et al*, 2005]](https://onlinelibrary.wiley.com/doi/abs/10.1111/j.1745-3992.2003.tb00134.x).  An assessment to comprehensively measure this space would be prohibitively long [[H. Wainer, R. Feinberg, 2015]](https://rss.onlinelibrary.wiley.com/doi/full/10.1111/j.1740-9713.2015.00797.x).

Consequently a *Bayesian-network* approach was selected for the *Mastery* DCM.  Bayesian networks are widely used in educational assessments [[R. G. Almond, *et al*, 2015]](https://www.springer.com/gp/book/9781493921249) and in cognitive diagnostics [[A. A. Rupp, *et al*, 2010]](https://amzn.to/39OgB1T). This approach provides flexibility to deal with missing and incomplete data. In fact, [[R. G. Almond, *et al*, 2015]](https://www.springer.com/gp/book/9781493921249) contains an oblique description to this sort of use case. It is explicitly suggested in [[Zimba, 2012]](https://achievethecore.org/content/upload/ccssmgraph.pdf).

The *Mastery* DCM's network model was based on vertical articulations of learning standards. Various education researchers have developed these for various contexts. Jason Zimba developed an [exhaustive graph for the Kindergrden through Eight-Grade](https://achievethecore.org/content/upload/ccssmgraph.pdf) [Common Core Learning Standards for Math](http://www.corestandards.org/Math/)(CCSS-Math).  The State of New York designed in a *coherence map* into its [Next-Generation Mathematics Learning Standards](t.ly/AjMd7). The [Next-Generation Science Standards](https://www.nextgenscience.org/) have some vertical alignments designed into them, also.

The figure below illustrates a particular application.  This is rendered using the [(Sensitivity Analysis, Modeling, Inference and More)](http://reasoning.cs.ucla.edu/samiam/) Bayesian-network tool developed by UCLA.  This was the *Mastery* DCM's first proof-of-concept. This illustrates a readiness assessemt.  We see how weaknesses in prerequisite learning standards influence a teacher's belief about an individual learner's readiness to engage the content coming next.

<p align="center">
<img width="750" src="./ConceptOfOperations/161021 SAMIAM Illustrative ScreenCap — Overall Knowledge State.png" align="center" >
</p>

## Technology realization of the *Mastery* DCM.

The diagram below — an activity diagram using Unfied Modeling Language (UML) syntax (e.g., [[Rumbaugh, *et al*, 2009]](https://amzn.to/3bUez1B)) — depicts the system workflow. This at best vaguely approximates the actual realization of the production version.  This Software Configuraiton Item (SWCI) decomposition of the [R-based prototype](https://github.com/hamlett-neil-ur/diagnostic_cognitive_model/tree/master/PrototypeSubroutinesInR) matches this quite closely.  Each "swim lane" corresponds to a sobroutine in [R-based prototype](https://github.com/hamlett-neil-ur/diagnostic_cognitive_model/tree/master/PrototypeSubroutinesInR).

<p align="center">
<img width="900" src="./ConceptOfOperations/Use Case 5︎⃣ Review class summary of learner  knowledge state for all learning standards in course scope .png" align="center" >
</p>

The production version was implemented in python. The [SQLAlchmey](https://www.sqlalchemy.org/) package was employed for the data input/output.  Most data-handling within the algorithm was instantiated using [pandas](https://pandas.pydata.org/pandas-docs/stable/index.html). The [pgmpy](https://github.com/pgmpy/pgmpy) and [pomegranate](https://pomegranate.readthedocs.io/en/latest/) packages provided the Bayesian-network calculations.

Bayesian networks allow us to solve for the marginal conditional probabilities for any vertex in a directed-acyclic graph given measurement of other vertices within a reasonable distance. The *Mastery* DCM used this to provide diagnostic estimates for each individual student given some evidence of learning.  It thereby addressed varying evidentiary coverage of the curriculum among students. 

Now, limitations in the python open-source packages presented obstacles to achieving system response-time performance. The [pgmpy](https://github.com/pgmpy/pgmpy) uses an *exact-inference* approach called *variable elimination*.  That exact inference is subject to compute-scaling challenges is well-known (e.g., [[D. Koller, N. Friedman, 2009]](https://amzn.to/39K4jaB)). The phonemenon is sometimes referred to as "exponential blow-up". 

This presented problems when a given learning standard had more than six or seven direct prerequisites. A limited number of instances in Zimba's [CCSS-Math](https://achievethecore.org/content/upload/ccssmgraph.pdf) progressions.  

The [pomegranate](https://pomegranate.readthedocs.io/en/latest/) package implements an *approximate-inference* approach called "Loopy Belief Propagation" (LBP). This circumvents exponential blow-up.  The [pomegranate](https://pomegranate.readthedocs.io/en/latest/) package however appeared to exhibit very slow numerical convergence under some conditions.  Specifically, when a vertex had more than three direct prerequisites whose conditional probabilties were identically distributed, [pomegranate](https://pomegranate.readthedocs.io/en/latest/)'s LBP could take an hour to converge on a specific calculation.

At the conclusion of the *Watson-classroom* initiative, alternative inference-calculation approaches were under development to circumvent these limitations.  The first attempt employed a techniqe called "soft separation".  The graph was broken apart in so that problematic vertices could be handled separately.  Then factor sum-product techniques would be applied to try to handle the calculation more-efficiently.

A more-sophisticated technique based on [fast Fourier transforms](https://mathworld.wolfram.com/FastFourierTransform.html) (FFT) was under investigation. That sum-product factor multiplication [[D. Koller, N. Friedman, 2009, §9.3]](https://amzn.to/39K4jaB)) resembles [discrete convolution](http://www.astro.rug.nl/~vdhulst/SignalProcessing/Hoorcolleges/college03.pdf) provided the essential insight. Under special circumstances, the sum-product operation can apparently be accelerated substantially through clever reformulation as a discret convolution. The calculation could then be performed much more rapidly via FFT.


## Client adoption.

Many technology innovations are as much about business process as about the technology itself.  *Watson Classroom* exemplifies this.  Deriving value of the *Mastery* DCM in particular was particularly dependent on a certain set of business practices.  Although the framework described above is well-grounded in the educational literature, the breadth and extent of application of these principles varied among clients.

Specifically, the *Mastery* DCM assumed a digitized, aligned curriculum.  Few educational systems were both digitized and aligned.  Consequently, an adoption workflow became necessary. 

<img width="750" src="./ClientAdoption/Offering Artifacts/190206 Offering Flow & Interdependencies v1-2.png" align="right" >

The figure to the right represents the adoption workflow at a high level.  It begins with an assessment of the extent of alignment and digitization of the curriculum. The framework by [[A. C. Porter, J. L Smithson, 2001]](https://repository.upenn.edu/cgi/viewcontent.cgi?article=1058&context=cpre_researchreports) provides provides a high-level guide.


The workflow subsequently focused on gettings digitized. Tooling provided outside the core *Watson-Classroom* solution appeared useful. For clients already embracing curriculum alignment, this might involve changing the tooling used. Many cases, this might involve shifting from Microsoft Office to more data-focused tool.

As digitization is accomplished, alignment indicators are developed. The essential indicators are much-more rudimentary than those in [[A. C. Porter, J. L Smithson, 2001]](https://repository.upenn.edu/cgi/viewcontent.cgi?article=1058&context=cpre_researchreports). However, the potential for surprise could occur when hard data reveals circumstances that differ from expectations.


