def convertDate(dt) -> str:
	'''
		params: `dt` (datetime object)

		returns: string
		
		Converts datetime object to string suitable for use in If-Modified-Since headers
	'''
	return f"{getDayOfWeekStr(dt.weekday())}, {dt.day:02d} {getMonthStr(dt.month)} {dt.year} {dt.hour:02d}:{dt.minute:02d}:{dt.second:02d} GMT"

def getDayOfWeekStr(day_num: int) -> str:
	match day_num:
		case 0:
			return "Mon"
		case 1:
			return "Tue"
		case 2:
			return "Wed"
		case 3:
			return "Thu"
		case 4:
			return "Fri"
		case 5:
			return "Sat"
		case 6:
			return "Sun"
		case _:
			return "err"

def getMonthStr(month_num: int) -> str:
	match month_num:
		case 1:
			return "Jan"
		case 2:
			return "Feb"
		case 3:
			return "Mar"
		case 4:
			return "Apr"
		case 5:
			return "May"
		case 6:
			return "Jun"
		case 7: 
			return "Jul"
		case 8:
			return "Aug"
		case 9:
			return "Sep"
		case 10:
			return "Oct"
		case 11:
			return "Nov"
		case 12:
			return "Dec"
		case _:
			return "err"

