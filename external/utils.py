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
                sorted(g_res['alternative'], key=lambda alternative: alternative.get("confidence"))
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
