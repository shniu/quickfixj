/*******************************************************************************
 * Copyright (c) quickfixengine.org  All rights reserved.
 *
 * This file is part of the QuickFIX FIX Engine
 *
 * This file may be distributed under the terms of the quickfixengine.org
 * license as defined by quickfixengine.org and appearing in the file
 * LICENSE included in the packaging of this file.
 *
 * This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
 * THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE.
 *
 * See http://www.quickfixengine.org/LICENSE for licensing information.
 *
 * Contact ask@quickfixengine.org if any conditions of this licensing
 * are not clear to you.
 ******************************************************************************/

package quickfix;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Corresponds to SessionTime in C++ code
 */
public class DefaultSessionSchedule implements SessionSchedule { 
    private static final int NOT_SET = -1;
    private static final Pattern TIME_PATTERN = Pattern.compile("(\\d{2}):(\\d{2}):(\\d{2})(.*)");
    private final TimeEndPoint startTime;
    private final TimeEndPoint endTime;
    private final boolean isNonStopSession;
    private final boolean isWeekdaySession;  // 默认是 false
    private final int[] weekdayOffsets;
    protected static final Logger LOG = LoggerFactory.getLogger(DefaultSessionSchedule.class);

    // SessionSchedule 用来跟踪当前的这个Session是否需要被重置，为什么需要 SessionSchedule ?
    // Session 也可以不重置，一直使用, 重置的主要内容是序列号和缓存的消息
    //  1. 如果一直不重置，可能会导致序列号一直增长，会很大，字节数多消息体大，这个原因可能不充分
    //  2. 还有一个原因可能是应对更多可能的场景，比如有可能某些业务场景只需要每个星期的某几天的某几个时间段需要同步数据，其他时间不需要
    //  3. 也有可能需要停机维护
    // 比如，每日调度来更新Session状态数据；每周调度来更新Session状态数据
    public DefaultSessionSchedule(SessionSettings settings, SessionID sessionID) throws ConfigError,
            FieldConvertError {

        // Session 是否永不重置，通过配置项 NonStopSession 控制
        isNonStopSession = settings.isSetting(sessionID, Session.SETTING_NON_STOP_SESSION) && settings.getBool(sessionID, Session.SETTING_NON_STOP_SESSION);
        TimeZone defaultTimeZone = getDefaultTimeZone(settings, sessionID);
        // 在 NonStopSession 的情况下，如果需要重置 Session 就需要应用层来控制
        if (isNonStopSession) {
            isWeekdaySession = false;
            weekdayOffsets = new int[0];
            startTime = endTime = new TimeEndPoint(NOT_SET, 0, 0, 0, defaultTimeZone);
            return;
        } else {
            // 是否配置了 Weekdays
            isWeekdaySession = settings.isSetting(sessionID, Session.SETTING_WEEKDAYS);
        }

        boolean startDayPresent = settings.isSetting(sessionID, Session.SETTING_START_DAY);
        boolean endDayPresent = settings.isSetting(sessionID, Session.SETTING_END_DAY);

        // 设置了 Weekdays 就不要设置 StartDay and EndDay
        // 设置了 Weekdays 后就放在 weekdayOffsets
        if (isWeekdaySession) {
            if (startDayPresent || endDayPresent )
                throw new ConfigError("Session " + sessionID + ": usage of StartDay or EndDay is not compatible with setting " + Session.SETTING_WEEKDAYS);

            String weekdayNames = settings.getString(sessionID, Session.SETTING_WEEKDAYS);
            if (weekdayNames.isEmpty())
                throw new ConfigError("Session " + sessionID + ": " + Session.SETTING_WEEKDAYS + " is empty");

            // Weekdays 的配置是通过 , 分割
            String[] weekdayNameArray = weekdayNames.split(",");
            weekdayOffsets = new int[weekdayNameArray.length];
            for (int i = 0; i < weekdayNameArray.length; i++) {
                weekdayOffsets[i] = DayConverter.toInteger(weekdayNameArray[i]);
            }
        } else {
            // 没有设置 Weekdays，就需要保证要么同时设置 StartDay and EndDay，要么就不设置
            weekdayOffsets = new int[0];

            if (startDayPresent && !endDayPresent) {
                throw new ConfigError("Session " + sessionID + ": StartDay used without EndDay");
            }

            if (endDayPresent && !startDayPresent) {
                throw new ConfigError("Session " + sessionID + ": EndDay used without StartDay");
            }
        }
        // 起始时间点
        startTime = getTimeEndPoint(settings, sessionID, defaultTimeZone, Session.SETTING_START_TIME, Session.SETTING_START_DAY);
        // 结束时间点
        endTime = getTimeEndPoint(settings, sessionID, defaultTimeZone, Session.SETTING_END_TIME, Session.SETTING_END_DAY);
        LOG.info("[{}] {}", sessionID, toString());
    }

    private TimeEndPoint getTimeEndPoint(SessionSettings settings, SessionID sessionID,
                                         TimeZone defaultTimeZone, String timeSetting, String daySetting) throws ConfigError,
            FieldConvertError {

        // Time 的设置
        Matcher matcher = TIME_PATTERN.matcher(settings.getString(sessionID, timeSetting));
        if (!matcher.find()) {
            throw new ConfigError("Session " + sessionID + ": could not parse time '"
                    + settings.getString(sessionID, timeSetting) + "'.");
        }

        return new TimeEndPoint(
                // 如果没有设置day，默认 -1
                getDay(settings, sessionID, daySetting, NOT_SET),
                // 时分秒
                Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)),
                Integer.parseInt(matcher.group(3)), getTimeZone(matcher.group(4), defaultTimeZone));
    }

    private TimeZone getDefaultTimeZone(SessionSettings settings, SessionID sessionID)
            throws ConfigError, FieldConvertError {
        TimeZone sessionTimeZone;
        if (settings.isSetting(sessionID, Session.SETTING_TIMEZONE)) {
            String sessionTimeZoneID = settings.getString(sessionID, Session.SETTING_TIMEZONE);
            sessionTimeZone = TimeZone.getTimeZone(sessionTimeZoneID);
            if ("GMT".equals(sessionTimeZone.getID()) && !"GMT".equals(sessionTimeZoneID)) {
                throw new ConfigError("Unrecognized time zone '" + sessionTimeZoneID
                        + "' for session " + sessionID);
            }
        } else {
            sessionTimeZone = TimeZone.getTimeZone("UTC");
        }
        return sessionTimeZone;
    }

    private TimeZone getTimeZone(String tz, TimeZone defaultZone) {
        return "".equals(tz) ? defaultZone : TimeZone.getTimeZone(tz.trim());
    }

    // 时间点的抽象
    private static class TimeEndPoint {
        private final int weekDay;
        private final int hour;
        private final int minute;
        private final int second;
        private final int timeInSeconds;
        private final TimeZone tz;

        public TimeEndPoint(int day, int hour, int minute, int second, TimeZone tz) {
            weekDay = day;
            this.hour = hour;
            this.minute = minute;
            this.second = second;
            this.tz = tz;
            timeInSeconds = timeInSeconds(hour, minute, second);
        }

        int getHour() {
            return hour;
        }

        int getMinute() {
            return minute;
        }

        int getSecond() {
            return second;
        }

        int getDay() {
            return weekDay;
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof TimeEndPoint) {
                TimeEndPoint otherTime = (TimeEndPoint) o;
                return timeInSeconds == otherTime.timeInSeconds;
            }
            return false;
        }

        public int hashCode() {
            assert false : "hashCode not supported";
            return 0;
        }

        TimeZone getTimeZone() {
            return tz;
        }
    }

    /**
     * find the most recent session date/time range on or before t
     * if t is in a session then that session will be returned
     * @param t specific date/time
     * @return relevant session date/time range
     */
    private TimeInterval theMostRecentIntervalBefore(Calendar t) {
        TimeInterval timeInterval = new TimeInterval();
        // 取 startTime 的设置
        Calendar intervalStart = timeInterval.getStart();
        intervalStart.setTimeZone(startTime.getTimeZone());
        intervalStart.setTimeInMillis(t.getTimeInMillis());
        intervalStart.set(Calendar.HOUR_OF_DAY, startTime.getHour());
        intervalStart.set(Calendar.MINUTE, startTime.getMinute());
        intervalStart.set(Calendar.SECOND, startTime.getSecond());
        intervalStart.set(Calendar.MILLISECOND, 0);

        // 取 endTime 的设置
        Calendar intervalEnd = timeInterval.getEnd();
        intervalEnd.setTimeZone(endTime.getTimeZone());
        intervalEnd.setTimeInMillis(t.getTimeInMillis());
        intervalEnd.set(Calendar.HOUR_OF_DAY, endTime.getHour());
        intervalEnd.set(Calendar.MINUTE, endTime.getMinute());
        intervalEnd.set(Calendar.SECOND, endTime.getSecond());
        intervalEnd.set(Calendar.MILLISECOND, 0);

        // Weekday 调度 ?
        if (isWeekdaySession) {
            while (intervalStart.getTimeInMillis() > t.getTimeInMillis() ||
                    !validDayOfWeek(intervalStart)) {
                intervalStart.add(Calendar.DAY_OF_WEEK, -1);
                intervalEnd.add(Calendar.DAY_OF_WEEK, -1);
            }

            if (intervalEnd.getTimeInMillis() <= intervalStart.getTimeInMillis()) {
                intervalEnd.add(Calendar.DAY_OF_WEEK, 1);
            }

        } else {
            if (isSet(startTime.getDay())) {
                intervalStart.set(Calendar.DAY_OF_WEEK, startTime.getDay());
                if (intervalStart.getTimeInMillis() > t.getTimeInMillis()) {
                    intervalStart.add(Calendar.WEEK_OF_YEAR, -1);
                    intervalEnd.add(Calendar.WEEK_OF_YEAR, -1);
                }
            } else if (intervalStart.getTimeInMillis() > t.getTimeInMillis()) {
                intervalStart.add(Calendar.DAY_OF_YEAR, -1);
                intervalEnd.add(Calendar.DAY_OF_YEAR, -1);
            }

            if (isSet(endTime.getDay())) {
                intervalEnd.set(Calendar.DAY_OF_WEEK, endTime.getDay());
                if (intervalEnd.getTimeInMillis() <= intervalStart.getTimeInMillis()) {
                    intervalEnd.add(Calendar.WEEK_OF_MONTH, 1);
                }
            } else if (intervalEnd.getTimeInMillis() <= intervalStart.getTimeInMillis()) {
                intervalEnd.add(Calendar.DAY_OF_WEEK, 1);
            }
        }

        return timeInterval;
    }

    // 对时间间隔的一个抽象，我们可以把时间划分为不同的间隔，包括起始和结束两个时间点
    private static class TimeInterval {
        private final Calendar start = SystemTime.getUtcCalendar();
        private final Calendar end = SystemTime.getUtcCalendar();

        // 传入的时间t在start和end之间，说明属于这个间隔
        boolean isContainingTime(Calendar t) {
            return t.compareTo(start) >= 0 && t.compareTo(end) <= 0;
        }

        public String toString() {
            return start.getTime() + " --> " + end.getTime();
        }

        // 重写 equals
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TimeInterval)) {
                return false;
            }
            TimeInterval otherInterval = (TimeInterval) other;
            // 起止时间点相同，说明是同一个间隔
            return start.equals(otherInterval.start) && end.equals(otherInterval.end);
        }

        public int hashCode() {
            assert false : "hashCode not supported";
            return 0;
        }

        Calendar getStart() {
            return start;
        }

        Calendar getEnd() {
            return end;
        }
    }

    // 主要用来判断是不是属于同一个 session
    // 使用时间来判断：当前时间和session的创建时间进行比较，而且通过调度器来检查
    @Override
    public boolean isSameSession(Calendar time1, Calendar time2) {
        if (isNonStopSession())
            return true;
        // 计算距离 time1 最近的间隔
        TimeInterval interval1 = theMostRecentIntervalBefore(time1);
        // 如果间隔不包含time1，说明time1属于一个更新的间隔，那么time1和time2肯定不属于同一个间隔
        if (!interval1.isContainingTime(time1)) {
            return false;
        }
        // 计算距离 time2 最近的间隔
        TimeInterval interval2 = theMostRecentIntervalBefore(time2);
        // 如果间隔2包含time2，且间隔1和间隔2是同一个间隔，表示是同一个间隔，否则不是同一个间隔
        return interval2.isContainingTime(time2) && interval1.equals(interval2);
    }

    @Override
    public boolean isNonStopSession() {
        return isNonStopSession;
    }

    private boolean isDailySession() {
        return !isSet(startTime.getDay()) && !isSet(endTime.getDay());
    }

    // 现在是否处于 session 的时间窗口内 (Interval)
    @Override
    public boolean isSessionTime() {
        if(isNonStopSession()) {
            return true;
        }
        Calendar now = SystemTime.getUtcCalendar();
        TimeInterval interval = theMostRecentIntervalBefore(now);
        return interval.isContainingTime(now);
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();

        SimpleDateFormat dowFormat = new SimpleDateFormat("EEEE");
        dowFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss-z");
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        TimeInterval ti = theMostRecentIntervalBefore(SystemTime.getUtcCalendar());

        formatTimeInterval(buf, ti, timeFormat, false);

        // Now the localized equivalents, if necessary
        if (!startTime.getTimeZone().equals(SystemTime.UTC_TIMEZONE)
                || !endTime.getTimeZone().equals(SystemTime.UTC_TIMEZONE)) {
            buf.append(" (");
            formatTimeInterval(buf, ti, timeFormat, true);
            buf.append(")");
        }

        return buf.toString();
    }

    private void formatTimeInterval(StringBuilder buf, TimeInterval timeInterval,
                                    SimpleDateFormat timeFormat, boolean local) {
        if (isWeekdaySession) {
            try {
                for (int i = 0; i < weekdayOffsets.length; i++) {
                    buf.append(DayConverter.toString(weekdayOffsets[i]));
                    buf.append(", ");
                }
            } catch (ConfigError ex) {
                // this can't happen as these are created using DayConverter.toInteger
            }
        } else if (!isDailySession()) {
            buf.append("weekly, ");
            formatDayOfWeek(buf, startTime.getDay());
            buf.append(" ");
        } else {
            buf.append("daily, ");
        }

        if (local) {
            timeFormat.setTimeZone(startTime.getTimeZone());
        }
        buf.append(timeFormat.format(timeInterval.getStart().getTime()));

        buf.append(" - ");

        if (!isDailySession()) {
            formatDayOfWeek(buf, endTime.getDay());
            buf.append(" ");
        }
        if (local) {
            timeFormat.setTimeZone(endTime.getTimeZone());
        }
        buf.append(timeFormat.format(timeInterval.getEnd().getTime()));
    }

    private void formatDayOfWeek(StringBuilder buf, int dayOfWeek) {
        try {
            String dayName = DayConverter.toString(dayOfWeek).toUpperCase();
            if (dayName.length() > 3) {
                dayName = dayName.substring(0, 3);
            }
            buf.append(dayName);
        } catch (ConfigError e) {
            buf.append("[Error: unknown day ").append(dayOfWeek).append("]");
        }
    }

    private int getDay(SessionSettings settings, SessionID sessionID, String key, int defaultValue)
            throws ConfigError, FieldConvertError {
        return settings.isSetting(sessionID, key) ?
                DayConverter.toInteger(settings.getString(sessionID, key))
                : NOT_SET;
    }

    private boolean isSet(int value) {
        return value != NOT_SET;
    }

    private static int timeInSeconds(int hour, int minute, int second) {
        return (hour * 3600) + (minute * 60) + second;
    }

    /**
     * is the startDateTime a valid day based on the permitted days of week
     * @param startDateTime time to test
     * @return flag indicating if valid
     */
    private boolean validDayOfWeek(Calendar startDateTime) {
        int dow = startDateTime.get(Calendar.DAY_OF_WEEK);
        for (int i = 0; i < weekdayOffsets.length; i++)
            if (weekdayOffsets[i] == dow)
                return true;
        return false;
    }
}
