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

class ratingMatrix(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                    reducer=self.reducer_ratings_by_user)]

    def mapper_parse_input(self, key, line):
        # Outputs userID => (movieID, rating)
        (userID, movieID, rating) = line.split(',')
        yield movieID, (userID, float(rating))

    def reducer_ratings_by_user(self, movieID, itemRatings):

        #Group (user, rating) pairs by userID

        for userID, rating in itemRatings:

            yield movieID, userID+':'+str(rating)

if __name__ == '__main__':
    ratingMatrix.run()