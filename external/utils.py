from subprocess import Popen, TimeoutExpired

import speech_recognition as recognition


def get_file_extension(logger, filename):
    try:
        return filename.rsplit('.', 1)[1].lower()
    except Exception as e:
        logger.error("Failed to retrieve file format for {}".format(filename))
        logger.error(e)
        return None


def convert_to_wav(logger, source_file_path, result_file_path):
    """
    Decode from any ffmpeg type to .wav
    :param logger:
    :param source_file_path:
    :param result_file_path:
    :return: boolean whether successful
    """
    process = ''
    try:
        process = Popen(['ffmpeg', '-y', '-i', source_file_path, '-f', 'wav', result_file_path])
        outs, errs = process.communicate(timeout=5)
        if process.returncode == 0:
            logger.info('Successfully converted .aac ti .wav')
            return True
    except (TimeoutExpired, Exception) as e:
        if e is TimeoutExpired:
            if process:
                process.kill()
            outs, errs = process.communicate()
            logger.error('Failed to convert .aac file. Reason')
            logger.error(errs)
        else:
            logger.error(e)
    return False


def retrieve_phrase(logger, path_to_file):
    r = recognition.Recognizer()
    r.operation_timeout = 5
    with recognition.AudioFile(path_to_file) as source:
        audio = r.record(source)
    result_list = []
    try:
        g_res = r.recognize_google(audio, show_all=True)
        logger.info(g_res)
        if g_res and g_res.get("alternative") is not None:
            result_list = [
                i.get('transcript')
                for i in
                reversed(sorted(g_res['alternative'], key=lambda alternative: alternative.get("confidence")))
            ]
    except Exception as e:
        logger.error('Failed to get recognized data from Google')
        logger.error(e)
    if not result_list:
        try:
            recognized_string = r.recognize_sphinx(audio)
            if recognized_string:
                result_list.append(recognized_string)
        except Exception as e:
            logger.error('Failed to get recognized data from Sphinx')
            logger.error(e)
    return result_list


def sub_splitter(s):
    l = int(len(s) / 2)
    split_index_upper = 0
    split_index_space = len(s)
    for i, ch in enumerate(s[l:]):
        if ch.isupper():
            split_index_upper = int(l + i)
            break
        if ch == ' ':
            split_index_space = min(split_index_space, int(l + i))

    splitter = int(split_index_space) if split_index_upper == 0 else int(split_index_upper)
    return [s[0:splitter].strip(), s[splitter:].strip()]


def get_lengths(ts):
    for sub_list in ts:
        if sub_list[1] - sub_list[0] > 19000:
            return [sub_list]

    ts.sort(key=lambda x: x[0] - x[1])
    ts = ts[:3]

    res = []
    for one in ts:
        if one[1] - one[0] < 6000:
            res.append([one[0] - 4000, one[1] + 4000, one[2]])
        else:
            res.append(one)

    return res
