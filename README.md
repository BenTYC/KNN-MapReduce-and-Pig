KNN-MapReduce-and-Pig
===

Overview 
---
Implement the 𝑘 nearest neighbors (KNN) query using MapReduce and Pig. In the KNN query, the input is a set of points (𝑃) in the Euclidean space, a query point (𝑞), and an integer (𝑘). The output is the 𝑘 points in 𝑃 that are closest to the query point 𝑞. A naïve solution is to compute the distance from each point 𝑝 ∈ 𝑃 to the query point 𝑞, sort all the points by their distance, and choose the top 𝑘 points. However, this naïve solution might not be directly applicable in MapReduce. Provide two implementations, one using a MapReduce program in Hadoop, and the other using a plain Pig Latin script.
