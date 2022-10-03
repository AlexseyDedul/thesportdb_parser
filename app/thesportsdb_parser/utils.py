import os
import re

import aiofiles
import aiohttp


async def save_img_to_folder(sv_path, cur_img, title):
    if not os.path.exists('./resource/img/'):
        os.makedirs('./resource/img/')
    folder = f'./resource/img{sv_path}'
    if not os.path.exists(folder):
        os.makedirs(folder)
    if cur_img:
        tmp = re.findall(r'.\w*', cur_img)
        ext = tmp[len(tmp) - 1]
        tmp_path = folder + title.replace(" ", "_").replace("/", "_").replace("'", "_").lower() + ext
        new_path = tmp_path.replace(ext, ".webp")
        if not os.path.exists(new_path):
            if not os.path.exists(tmp_path):
                async with aiohttp.ClientSession() as session:
                    async with session.get(cur_img) as res:
                        if res.status == 200:
                            f = await aiofiles.open(tmp_path, mode='wb')
                            await f.write(await res.read())
                            await f.close()

                        cmd = ("cwebp -q " + str(100) + " " + tmp_path + " -o " + tmp_path[:tmp_path.index(".") - 4] + ".webp")

                        os.system(cmd)

                        os.remove(tmp_path)

                        return new_path.replace('.', '', 1)
        else:
            return new_path.replace('.', '', 1)

    return None