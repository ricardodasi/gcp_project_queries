
###############################################################################################################


#Queries for data tables 


###############################################################################################################

#######################################

#list of collections 


# 1	event_cover_redemptions

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.event_id') AS event_id
			,JSON_EXTRACT(document,'$.redemption_date') AS redemption_date
			,JSON_EXTRACT(document,'$.user_id') AS user_id

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number  

	FROM  events.raw_events

	WHERE document_collection = 'event_cover_redemptions'

)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 2	event_cover_redemptions_housekeeping

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.event_date') AS event_date
			,JSON_EXTRACT(document,'$.event_id') AS event_id
			,JSON_EXTRACT(document,'$.remaining_gou_covers')AS remaining_gou_covers

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number  

	FROM  events.raw_events

	WHERE document_collection = 'event_cover_redemptions_housekeeping'

)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1
	

# 3	event_likes

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.event_id') AS event_id #string
			,JSON_EXTRACT(document,'$.user_id') AS user_id #string

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number  


	FROM  events.raw_events

	WHERE document_collection = 'event_likes'

)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1



# 4 event_rating_review_stats

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.AMBIENTE_TOP') AS ambiente_top #int
			,JSON_EXTRACT(document,'$.BUENO_BONITO_BARATO') AS bueno_bonito_barato #int

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number  

	FROM  events.raw_events

	WHERE document_collection = 'event_rating_review_stats'
)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1




# 5 event_rating_reviews

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.createdAt') AS created_at #str
			,JSON_EXTRACT(document,'$.event_id') AS event_id #str
			,JSON_EXTRACT(document,'$.rating') AS rating #int 
			,JSON_EXTRACT(document,'$.review') AS review # str
			,JSON_EXTRACT(document,'$.user_id') AS event_rating_user_id #str
			,JSON_EXTRACT(document,'$.user_name') AS user_name #str
			,JSON_EXTRACT(document,'$.concepts') AS concepts # str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number

	FROM  events.raw_events

	WHERE document_collection = 'event_rating_reviews'

)


SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 6	events

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.description') AS event_name
			,JSON_EXTRACT(document,'$.init_date') AS init_date 
			,JSON_EXTRACT(document,'$.local.name') AS local_name
			,JSON_EXTRACT(document,'$.localId') AS local_id

			,JSON_EXTRACT(document,'$.oneDayPlan.opens') AS one_day_plan_opening_time
			,JSON_EXTRACT(document,'$.oneDayPlan.closes') AS one_day_plan_closing_time
			,JSON_EXTRACT(document,'$.oneDayPlan.cover.amount')  AS cover_price
			,JSON_EXTRACT(document,'$.oneDayPlan.cover.currency') AS currency

			,JSON_EXTRACT(document,'$.local.schedule.monday.opens') AS monday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.monday.closes') AS monday_closing_time	

			,JSON_EXTRACT(document,'$.local.schedule.tuesday.opens') AS tuesday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.tuesday.closes') AS tuesday_closing_time	

			,JSON_EXTRACT(document,'$.local.schedule.wednesday.opens') AS wednesday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.wednesday.closes') AS wednesday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.thursday.opens') AS thursday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.thursday.closes') AS thursday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.friday.opens') AS friday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.friday.closes') AS friday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.saturday.opens') AS saturday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.saturday.closes') AS saturday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.sunday.opens') AS sunday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.sunday.closes') AS sunday_closing_time


			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number


	FROM  events.raw_events

	WHERE document_collection = 'events'


)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1



# 7	experience


WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.init_date') AS init_date #str
			,JSON_EXTRACT(document,'$.liqueur_type') AS liqueur_type #str
			,JSON_EXTRACT(document,'$.local.name') AS local_name #str
			,JSON_EXTRACT(document,'$.thatsIncluded') AS thats_included #str
			,JSON_EXTRACT(document,'$.toDo') AS to_do #str

			,JSON_EXTRACT(document,'$.oneDayPlan.opens') AS one_day_plan_opening_time
			,JSON_EXTRACT(document,'$.oneDayPlan.closes') AS one_day_plan_closing_time
			,JSON_EXTRACT(document,'$.oneDayPlan.cover.amount')  AS cover_price
			,JSON_EXTRACT(document,'$.oneDayPlan.cover.currency') AS currency

			,JSON_EXTRACT(document,'$.local.schedule.monday.opens') AS monday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.monday.closes') AS monday_closing_time	

			,JSON_EXTRACT(document,'$.local.schedule.tuesday.opens') AS tuesday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.tuesday.closes') AS tuesday_closing_time	

			,JSON_EXTRACT(document,'$.local.schedule.wednesday.opens') AS wednesday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.wednesday.closes') AS wednesday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.thursday.opens') AS thursday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.thursday.closes') AS thursday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.friday.opens') AS friday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.friday.closes') AS friday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.saturday.opens') AS saturday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.saturday.closes') AS saturday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.sunday.opens') AS sunday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.sunday.closes') AS sunday_closing_time


			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number

	FROM  events.raw_events

	WHERE document_collection = 'experience'

)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 8 guest_lists


WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.name') AS event_name #str
			,JSON_EXTRACT(document,'$.localId') AS local_id #str
			,JSON_EXTRACT(document,'$.ownerId') AS owner_id #str
			,JSON_EXTRACT(document,'$.date') AS event_date #str
			,JSON_EXTRACT(document,'$.guests') AS guest_list #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number

	FROM  events.raw_events

	WHERE document_collection = 'guest_lists'
)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1



# 9 local_cover_redemptions

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.local_id') AS local_id #str
			,JSON_EXTRACT(document,'$.redemption_date') AS redemption_date #str
			,JSON_EXTRACT(document,'$.user_id') AS user_id #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number

	FROM  events.raw_events

	WHERE document_collection = 'local_cover_redemptions'

)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 10 local_cover_redemptions_housekeeping

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.date') AS cover_redemption_date #str
			,JSON_EXTRACT(document,'$.local_id') AS local_id #str
			,JSON_EXTRACT(document,'$.remaining_gou_covers') AS remaining_gou_covers #float 

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number



	FROM  events.raw_events

	WHERE document_collection = 'local_cover_redemptions_housekeeping'

)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1



# 11 local_likes


WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.local_id') AS local_id #str
			,JSON_EXTRACT(document,'$.user_id') AS user_id #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number


	FROM  events.raw_events

	WHERE document_collection = 'local_likes'

)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1



# 12 local_rating_review_stats

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.AMBIENTE_TOP') AS ambiente_top #str
			,JSON_EXTRACT(document,'$.BUENO_BONITO_BARATO') AS bueno_bonito_barato #str
			,JSON_EXTRACT(document,'$.CONCEPTO_COOL') AS concepto_cool #str
			,JSON_EXTRACT(document,'$.QUE_NIVEL_DE_TRAGO') AS que_nivel_de_trago #str
			,JSON_EXTRACT(document,'$.VIVA_LA_MUSIQUE') AS viva_la_musique #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number			

	FROM  events.raw_events

	WHERE document_collection = 'local_rating_review_stats'

)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1




# 13 local_rating_reviews

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.user_id') AS user_id #str
			,JSON_EXTRACT(document,'$.concepts') AS concepts #str
			,JSON_EXTRACT(document,'$.createdAt') AS review_created_at #str
			,JSON_EXTRACT(document,'$.local_id') AS local_id #str
			,JSON_EXTRACT(document,'$.rating') AS rating #int

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number	

	FROM  events.raw_events

	WHERE document_collection = 'local_rating_reviews'

)

SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1



# 14 locals

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.name') AS local_name #str
			,JSON_EXTRACT(document,'$.discountPercentage') AS discount_percentage #int
			,JSON_EXTRACT(document,'$.musicGenre.keyword') AS music_genre_keyword #str
			,JSON_EXTRACT(document,'$.musicGenre.schedule') AS schedule #str

			,JSON_EXTRACT(document,'$.local.schedule.monday.opens') AS monday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.monday.closes') AS monday_closing_time	

			,JSON_EXTRACT(document,'$.local.schedule.tuesday.opens') AS tuesday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.tuesday.closes') AS tuesday_closing_time	

			,JSON_EXTRACT(document,'$.local.schedule.wednesday.opens') AS wednesday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.wednesday.closes') AS wednesday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.thursday.opens') AS thursday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.thursday.closes') AS thursday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.friday.opens') AS friday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.friday.closes') AS friday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.saturday.opens') AS saturday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.saturday.closes') AS saturday_closing_time

			,JSON_EXTRACT(document,'$.local.schedule.sunday.opens') AS sunday_opening_time
			,JSON_EXTRACT(document,'$.local.schedule.sunday.closes') AS sunday_closing_time


			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number	


	FROM  events.raw_events

	WHERE document_collection = 'locals'


)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1



# 15 moods


WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.color') AS color #str
			,JSON_EXTRACT(document,'$.image_url') AS image_url #str
			,JSON_EXTRACT(document,'$.keyword') AS mood_keyword #str
			,JSON_EXTRACT(document,'$.name') AS mood_name #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number	


	FROM  events.raw_events

	WHERE document_collection = 'moods'


)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 16 music_genres

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.keyword') AS music_genre_keyword #str
			,JSON_EXTRACT(document,'$.name') AS music_genre_name #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number	


	FROM  events.raw_events

	WHERE document_collection = 'music_genres'

)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1



# 17 owners

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.id') AS owner_id #str
			,JSON_EXTRACT(document,'$.name') AS owner_name #str
			,JSON_EXTRACT(document,'$.lastname') AS owner_last_name #str
			,JSON_EXTRACT(document,'$.ownerRole') AS owner_role #str
			,JSON_EXTRACT(document,'$.ownerType') AS owner_type #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number	


	FROM  events.raw_events

	WHERE document_collection = 'owners'


)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1



# 18 promotion_event

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.init_date') AS init_date #str
			,JSON_EXTRACT(document,'$.init_time') AS init_time #str
			,JSON_EXTRACT(document,'$.end_date') AS end_date #str
			,JSON_EXTRACT(document,'$.end_time') AS end_time #str 
			,JSON_EXTRACT(document,'$.max_events') AS max_events #int
			,JSON_EXTRACT(document,'$.number_of_promotions') AS number_of_promotions #int 
			,JSON_EXTRACT(document,'$.promotion_id') AS promotion_id #str
			,JSON_EXTRACT(document,'$.plan') AS scheduled_plan	#str	

			,JSON_EXTRACT(document,'$.plan.monday.init') AS monday_init_time
			,JSON_EXTRACT(document,'$.plan.monday.end') AS monday_end_time

			,JSON_EXTRACT(document,'$.plan.tuesday.init') AS tuesday_init_time
			,JSON_EXTRACT(document,'$.plan.tuesday.end') AS tuesday_end_time

			,JSON_EXTRACT(document,'$.plan.wednesday.init') AS wednesday_init_time
			,JSON_EXTRACT(document,'$.plan.wednesday.end') AS wednesday_end_time

			,JSON_EXTRACT(document,'$.plan.thursday.init') AS thursday_init_time
			,JSON_EXTRACT(document,'$.plan.thursday.end') AS thursday_end_time

			,JSON_EXTRACT(document,'$.plan.friday.init') AS friday_init_time
			,JSON_EXTRACT(document,'$.plan.friday.end') AS friday_end_time

			,JSON_EXTRACT(document,'$.plan.saturday.init') AS saturday_init_time
			,JSON_EXTRACT(document,'$.plan.saturday.end') AS saturday_end_time

			,JSON_EXTRACT(document,'$.plan.sunday.init') AS sunday_init_time
			,JSON_EXTRACT(document,'$.plan.sunday.end') AS sunday_end_time


			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number	

	FROM  events.raw_events

	WHERE document_collection = 'promotion_event'

)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 19 promotion_local

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.local_id') AS local_id #str
			,JSON_EXTRACT(document,'$.init_date') AS init_date #str
			,JSON_EXTRACT(document,'$.init_time') AS init_time #str
			,JSON_EXTRACT(document,'$.end_date') AS end_date #str
			,JSON_EXTRACT(document,'$.end_time') AS end_time #str 
			,JSON_EXTRACT(document,'$.max_events') AS max_events #int
			,JSON_EXTRACT(document,'$.promotion_id') AS promotion_id #str
			,JSON_EXTRACT(document,'$.remaining_events') AS remaining_events #int
			,JSON_EXTRACT(document,'$.remaining_number_of_promotions') AS remaining_number_of_promotions #int 
			,JSON_EXTRACT(document,'$.plan') AS scheduled_plan #str

			,JSON_EXTRACT(document,'$.plan.monday.init') AS monday_init_time
			,JSON_EXTRACT(document,'$.plan.monday.end') AS monday_end_time

			,JSON_EXTRACT(document,'$.plan.tuesday.init') AS tuesday_init_time
			,JSON_EXTRACT(document,'$.plan.tuesday.end') AS tuesday_end_time

			,JSON_EXTRACT(document,'$.plan.wednesday.init') AS wednesday_init_time
			,JSON_EXTRACT(document,'$.plan.wednesday.end') AS wednesday_end_time

			,JSON_EXTRACT(document,'$.plan.thursday.init') AS thursday_init_time
			,JSON_EXTRACT(document,'$.plan.thursday.end') AS thursday_end_time

			,JSON_EXTRACT(document,'$.plan.friday.init') AS friday_init_time
			,JSON_EXTRACT(document,'$.plan.friday.end') AS friday_end_time

			,JSON_EXTRACT(document,'$.plan.saturday.init') AS saturday_init_time
			,JSON_EXTRACT(document,'$.plan.saturday.end') AS saturday_end_time

			,JSON_EXTRACT(document,'$.plan.sunday.init') AS sunday_init_time
			,JSON_EXTRACT(document,'$.plan.sunday.end') AS sunday_end_time

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number	

	FROM  events.raw_events

	WHERE document_collection = 'promotion_local'


)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 20 promotion_local_redemption_housekeeping

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.remaining_promotions') AS remaining_promotions #int 

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number

	FROM  events.raw_events

	WHERE document_collection = 'promotion_local_redemption_housekeeping'


)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1




# 21 promotion_local_redemptions

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.promotion_local_id') AS promotion_local_id #str
			,JSON_EXTRACT(document,'$.redemption_date') AS redemption_date #str
			,JSON_EXTRACT(document,'$.user_id') AS user_id #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									) AS row_number

	FROM  events.raw_events

	WHERE document_collection = 'promotion_local_redemptions'
)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1





# 22 promotions

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.name') AS promotion_name #str
			,JSON_EXTRACT(document,'$.createAt') AS created_at #str
			,JSON_EXTRACT(document,'$.gouPromotions') AS gou_promotions #int 
			,JSON_EXTRACT(document,'$.ownerId') AS owner_id #str
			,JSON_EXTRACT(document,'$.type.type') AS type #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									)	 AS row_number

	FROM  events.raw_events

	WHERE document_collection = 'promotions'

) 
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1

# 23 tags

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.keyword') AS keyword #str
			,JSON_EXTRACT(document,'$.name') AS name #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									)	 AS row_number

	FROM  events.raw_events

	WHERE document_collection = 'tags'

)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 24 user_friends

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.user_id') AS user_id #str
			,JSON_EXTRACT(document,'$.friend.id') AS friend_id #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									)	 AS row_number


	FROM  events.raw_events

	WHERE document_collection = 'user_friends'

)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 25 users

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.age') AS age #int
			,JSON_EXTRACT(document,'$.birthdate') AS birthdate #str
			,JSON_EXTRACT(document,'$.city') AS city #str 
			,JSON_EXTRACT(document,'$.gender') AS gender #str
			,JSON_EXTRACT(document,'$.job') AS job #str
			,JSON_EXTRACT(document,'$.name') AS name #str 
			,JSON_EXTRACT(document,'$.university') AS university #str

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									)	 AS row_number


	FROM  events.raw_events

	WHERE document_collection = 'users'

)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


# 26  zones

WITH latest_events AS (

	SELECT document_id
			,timestamp
			,JSON_EXTRACT(document,'$.keyword') AS keyword
			,JSON_EXTRACT(document,'$.name') AS name

			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									)	 AS row_number

	FROM  events.raw_events

	WHERE document_collection = 'zones'
)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1