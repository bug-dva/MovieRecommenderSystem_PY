# To run locally:
# !python MovieRecommender.py --items=input/movie_titles.csv input/ratingData.txt > sims.txt

# To run on a single EMR node:
# !python MovieRecommender.py -r emr --items=input/movie_titles.csv input/ratingData.txt

# To run on 4 EMR nodes:
#!python MovieRecommender.py -r emr --num-ec2-instances=4 --items=input/movie_titles.csv input/ratingData.txt

# Troubleshooting EMR jobs (subsitute your job ID):
# !python -m mrjob.tools.emr.fetch_logs --find-failure j-1NXMMBNEQHAFT


from mrjob.job import MRJob
from mrjob.step import MRStep

from itertools import combinations

class MovieRecommender(MRJob):


    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                    reducer=self.reducer_count_relation),
            MRStep(mapper=self.mapper_sort_similarities,
                    reducer=self.reducer_output_similarities)]

    #MPStep1: divide data by userID

    def mapper_parse_input(self, key, line):
        # Outputs userID => (movieID, rating)
        (userID, movieID, rating) = line.split(',')
        yield userID, (movieID, float(rating))

    def reducer_ratings_by_user(self, user_id, itemRatings):
        #Group (item, rating) pairs by userID

        ratings = []
        for movieID, rating in itemRatings:
            ratings.append((movieID, rating))

        yield user_id, ratings

    #MPStep2: create movie pairs - build co-occurrence matrix

    def mapper_create_item_pairs(self, user_id, itemRatings):
        # Find every pair of movies each user has seen, and emit
        # each pair with its associated ratings

        # "combinations" finds every possible pair from the list of movies
        # this user viewed.

        for itemRating1, itemRating2 in combinations(itemRatings, 2):
            movieID1 = itemRating1[0]
            movieID2 = itemRating2[0]

            # Produce both orders so sims are bi-directional
            yield (movieID1, movieID2), 1
            yield (movieID2, movieID1), 1


    def reducer_count_relation(self, moviePair, value):
        yield moviePair, sum(value)

    #MPStep3: normalize
    def mapper_sort_similarities(self, moviePair, relation):
        # print(type(moviePair))
        # print(type(relation))
        movie1, movie2 = moviePair

        yield movie1, movie2+':'+str(relation)

    def reducer_output_similarities(self, movieID, relation):


        sum = 0
        output_dict = {}

        for record in relation:
            movie2 = record.split(':')[0]
            relation_count = int(record.split(':')[1])
            output_dict[movie2] = relation_count
            sum = sum + int(record.split(':')[1])

        for key in output_dict:
            movie2 = key
            output_value = float(output_dict[key]/sum)
            yield movieID, movie2+'='+str(output_value)


if __name__ == '__main__':
    MovieRecommender.run()