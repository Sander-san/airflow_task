queries = [
    [
        {"$sort": {"thumbsUpCount": -1}},
        {"$limit": 5},
        {"$project": {"_id": 0,
                      "reviewId": 1,
                      "userName": 1,
                      "content": 1,
                      "score": 1,
                      "thumbsUpCount": 1}}
    ],
    [
        {"$match": {"$expr": {"$lt": [{"$strLenCP": "$content"}, 5]}}},
        {"$project": {"_id": 0}}
    ],
    [
        {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdDate"}},
                    "average_score": {"$avg": "$score"}}},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}},
                      "average_score": 1}}
    ]
]
