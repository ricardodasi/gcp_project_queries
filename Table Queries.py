
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

			,JSON_EXTRACT(document,'$.schedule.monday.opens') AS monday_opening_time
			,JSON_EXTRACT(document,'$.schedule.monday.closes') AS monday_closing_time	

			,JSON_EXTRACT(document,'$.schedule.tuesday.opens') AS tuesday_opening_time
			,JSON_EXTRACT(document,'$.schedule.tuesday.closes') AS tuesday_closing_time	

			,JSON_EXTRACT(document,'$.schedule.wednesday.opens') AS wednesday_opening_time
			,JSON_EXTRACT(document,'$.schedule.wednesday.closes') AS wednesday_closing_time

			,JSON_EXTRACT(document,'$.schedule.thursday.opens') AS thursday_opening_time
			,JSON_EXTRACT(document,'$.schedule.thursday.closes') AS thursday_closing_time

			,JSON_EXTRACT(document,'$.schedule.friday.opens') AS friday_opening_time
			,JSON_EXTRACT(document,'$.schedule.friday.closes') AS friday_closing_time

			,JSON_EXTRACT(document,'$.schedule.saturday.opens') AS saturday_opening_time
			,JSON_EXTRACT(document,'$.schedule.saturday.closes') AS saturday_closing_time

			,JSON_EXTRACT(document,'$.schedule.sunday.opens') AS sunday_opening_time
			,JSON_EXTRACT(document,'$.schedule.sunday.closes') AS sunday_closing_time

			,JSON_EXTRACT(document,'$.schedule.sunday.closes') AS sunday_closing_time

			,JSON_EXTRACT(document,'$.zone.name') AS zone_name
			,JSON_EXTRACT(document,'$.mood.name') AS mood_name


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


# 27  statics

WITH latest_events AS (

	SELECT document_id
			,timestamp

			#all moods extractions 

			,JSON_EXTRACT(document,'$.moods.karaoke') AS mood_karaoke #int
			,JSON_EXTRACT(document,'$.moods.after Office') AS mood_after_office #int
			,JSON_EXTRACT(document,'$.moods.el Pre') AS mood_el_pre #int
			,JSON_EXTRACT(document,'$.moods.cervezas') AS mood_cervezas #int
			,JSON_EXTRACT(document,'$.moods.cocktails') AS mood_cocktails #int
			,JSON_EXTRACT(document,'$.moods.rooftop') AS mood_rooftop #int
			,JSON_EXTRACT(document,'$.moods.happy Hour') AS	mood_happy_hour #int
			,JSON_EXTRACT(document,'$.moods.rumba') AS mood_rumba #int 
			,JSON_EXTRACT(document,'$.moods.hasta las 5am')  AS mood_hasta_las_5_am #int

			#all music genres extraction 

			,JSON_EXTRACT(document,'$.musicGenre.jazz') AS music_genre_jazz #int
			,JSON_EXTRACT(document,'$.musicGenre.reggaetón') AS music_genre_reggaeton #int
			,JSON_EXTRACT(document,'$.musicGenre.crossover') AS music_genre_crossover #int 
			,JSON_EXTRACT(document,'$.musicGenre.brasileña') AS music_genre_brasilena #int
			,JSON_EXTRACT(document,"$.musicGenre.80s - 90s") AS music_genre_8090s #int  #REVIEW ORIGINAL FIELD NAME: 80's - 90's
			,JSON_EXTRACT(document,'$.musicGenre.latina') AS music_genre_latina #int 
			,JSON_EXTRACT(document,'$.musicGenre.mexicana/ranchera') AS music_genre_mexicana_ranchera #int #REVIEW ORIGINAL FIELD NAME: Mexicana/Ranchera'
			,JSON_EXTRACT(document,'$.musicGenre.champeta') AS music_genre_champeta #int
			,JSON_EXTRACT(document,'$.musicGenre.lounge') AS music_genre_lounge #int 
			,JSON_EXTRACT(document,'$.musicGenre.electrónica') AS music_genre_electronica #int 
			,JSON_EXTRACT(document,'$.musicGenre.rock') AS music_genre_rock #int
			,JSON_EXTRACT(document,'$.musicGenre.indie') AS music_genre_indie #int

			#all zones extraction 

			,JSON_EXTRACT(document,'$.zones.zona g') AS zones_zona_g #int  			
			,JSON_EXTRACT(document,'$.zones.quinta camacho') AS zones_quinta_camacho #int
			,JSON_EXTRACT(document,'$.zones.engativá') AS  zones_engativa #int 
			,JSON_EXTRACT(document,'$.zones.chapinero') AS zones_chapinero #int 
			,JSON_EXTRACT(document,'$.zones.chía') AS zones_chia #int
			,JSON_EXTRACT(document,'$.zones.la 93') AS zones_la_93 #int 
			,JSON_EXTRACT(document,'$.zones.usaquén') AS zones_usaquen #int
			,JSON_EXTRACT(document,'$.zones.cedritos') AS zones_cedritos #int 
			,JSON_EXTRACT(document,'$.zones.la 85') AS zones_la_85 #int 
			,JSON_EXTRACT(document,'$.zones.zona t') AS zones_zona_t #int
			,JSON_EXTRACT(document,'$.zones.galerías') AS zones_galerias #int 
			,JSON_EXTRACT(document,'$.zones.san felipe') AS zones_san_felipe #int
			,JSON_EXTRACT(document,'$.zones.teusaquillo') AS zones_teusaquillo #int 
			,JSON_EXTRACT(document,'$.zones.centro') AS zones_centro #int 


			,ROW_NUMBER() OVER (PARTITION BY document_id
									ORDER BY timestamp DESC 
									)	 AS row_number


	FROM  events.raw_events

	WHERE document_collection = 'statics'

)
SELECT * EXCEPT (row_number)
FROM latest_events
WHERE row_number = 1


###########################################################################################


#Queries for requested dashboards


#tabla para promociones con demografia para data studio tabla 1

WITH promotion_redention_with_users AS(

	SELECT locals.promotion_id,
			locals.local_id,
			locals.init_time,
			locals.end_time,
			locals.init_date,
			locals.end_date,

			redention.promotion_local_id,
			redention.redemption_date,
			redention.user_id,

			users_staging.gender,
			users_staging.age,
			users_staging.city,
			users_staging.university,

			zone.zone_name,
			zone.zone_mood,
			zone.name

	FROM staging.promotion_local_redemptions_staging AS redention 

	LEFT JOIN staging.users_staging  users ON  redention.user_id = users.document_id  

	JOIN staging.promotion_local_staging  locals ON redention.promotion_local_id = locals.document_id 

	JOIN staging.locals zone ON locals.local_id = zone.document_id

)

SELECT promotions.promotion_name,
		promotions.type,

		user_redentions.redemption_date,
		user_redentions.user_id,
		user_redentions.gender,
		user_redentions.age,
		user_redentions.city,
		user_redentions.university,
		user_redentions.zone_name,
		user_redentions.zone_mood,
		user_redentions.name,
		user_redentions.init_time,
		user_redentions.end_time,
		user_redentions.init_date,
		user_redentions.end_date,


FROM staging.promotion_staging AS promotions

JOIN  promotion_redention_with_users AS user_redentions ON promotions.document_id = user_redentions.promotion_id


#tabla 2 arroja cada evento de like con el nombre del local y el nombre del evento


WITH local_evento AS(

	SELECT events.event_name,
			events.local_id

	FROM  staging.events_staging AS events

	JOIN staging.event_likes_staging likes ON events.document_id = likes.event_id 


)

SELECT locals.local_name,
		local_evento.event_name,

FROM staging.locals_staging AS  locals 

JOIN local_evento ON locals.document_id = local_evento.local_id 


#tabla 3 hacer likes por local